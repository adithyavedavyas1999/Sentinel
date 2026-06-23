"""End-to-end tests for the FastAPI incident surface.

Tests use FastAPI's ``TestClient`` so we hit the real route handlers
(not just the underlying functions). The ``IncidentStore`` is pointed
at a tmp_path sqlite via ``SENTINEL_INCIDENTS_DB``, and the storage
factory is monkeypatched to the in-memory ``FakeStorage`` from
conftest.
"""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from sentinel.api import main as api_module
from sentinel.quality.incidents import Incident, IncidentStore


@pytest.fixture(autouse=True)
def _isolate_incidents(tmp_path, monkeypatch):
    monkeypatch.setenv("SENTINEL_INCIDENTS_DB", str(tmp_path / "i.sqlite"))
    monkeypatch.setenv("SENTINEL_CHAOS_STATE_PATH", str(tmp_path / "chaos.json"))


@pytest.fixture
def client(fake_storage, monkeypatch) -> TestClient:
    monkeypatch.setattr(api_module, "_storage_factory", lambda: fake_storage)
    return TestClient(api_module.create_app())


@pytest.fixture
def store() -> IncidentStore:
    return IncidentStore()


def _insert(store, **overrides):
    return store.insert(
        Incident(
            asset_key=overrides.pop("asset_key", "bronze/tlc_yellow"),
            partition_key=overrides.pop("partition_key", "2024-04-01"),
            error_type=overrides.pop("error_type", "ChaosTriggered"),
            error_message=overrides.pop("error_message", "chaos:tlc_5xx"),
            **overrides,
        )
    )


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_allowlist_endpoint_matches_module(client):
    from sentinel.agent.remediation import allowlist

    r = client.get("/allowlist")
    assert r.status_code == 200
    assert r.json() == allowlist()


def test_list_incidents_empty(client):
    r = client.get("/incidents")
    assert r.status_code == 200
    assert r.json() == []


def test_list_and_get_round_trip(client, store, fake_storage):
    iid = _insert(
        store,
        recent_metadata={"run_id": "r1"},
        upstream_lineage=["bronze.tlc_yellow"],
    )

    # List shows it
    r = client.get("/incidents")
    assert r.status_code == 200
    items = r.json()
    assert len(items) == 1
    assert items[0]["id"] == iid
    assert items[0]["asset_key"] == "bronze/tlc_yellow"

    # And detail rehydrates the JSON metadata
    r = client.get(f"/incidents/{iid}")
    assert r.status_code == 200
    detail = r.json()
    assert detail["upstream_lineage"] == ["bronze.tlc_yellow"]
    assert detail["recent_metadata"] == {"run_id": "r1"}
    # No report yet
    assert detail["incident_report"] is None


def test_get_incident_404(client):
    r = client.get("/incidents/does-not-exist")
    assert r.status_code == 404


def test_incident_detail_loads_report_from_storage(client, store, fake_storage):
    iid = _insert(store)
    report = {"incident_id": iid, "diagnosis": {"category": "upstream_outage"}}
    fake_storage.put_bytes(
        "sentinel-incidents",
        f"incidents/{iid}.json",
        json.dumps(report).encode(),
        content_type="application/json",
    )

    r = client.get(f"/incidents/{iid}")
    assert r.status_code == 200
    body = r.json()
    assert body["incident_report"]["diagnosis"]["category"] == "upstream_outage"


def test_resolve_endpoint_changes_status(client, store):
    iid = _insert(store)
    r = client.post(f"/incidents/{iid}/resolve")
    assert r.status_code == 200
    assert r.json()["status"] == "resolved"

    # And it's no longer in /incidents (list_open filter)
    r = client.get("/incidents")
    assert r.json() == []


def test_resolve_unknown_id_404(client):
    r = client.post("/incidents/nope/resolve")
    assert r.status_code == 404


def test_approve_off_allowlist(client, store):
    iid = _insert(store)
    r = client.post(
        f"/incidents/{iid}/approve",
        json={"action": "rewrite-the-model"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "off_allowlist"
    assert "Allowed" in body["detail"]


def test_approve_executes_allowlisted_action(client, store):
    iid = _insert(store)
    r = client.post(
        f"/incidents/{iid}/approve",
        json={"action": "retry-with-backoff", "note": "approved by alice"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "applied"
    assert "queued re-materialization" in body["detail"]
    # And the row got stamped
    row = store.get(iid)
    assert row["proposed_fix"] == "retry-with-backoff"


def test_approve_guard_failure(client, store):
    # silver/* is not eligible for retry-with-backoff (guard rejects).
    iid = _insert(
        store,
        asset_key="silver/trips_weather",
        error_type="HTTPStatusError",
        error_message="503",
    )
    r = client.post(
        f"/incidents/{iid}/approve",
        json={"action": "retry-with-backoff"},
    )
    assert r.status_code == 200
    assert r.json()["status"] == "guard_failed"


def test_approve_unknown_incident_404(client):
    r = client.post("/incidents/nope/approve", json={"action": "retry-with-backoff"})
    assert r.status_code == 404
