from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from sentinel.agent import context
from sentinel.agent.embeddings import IncidentIndex, SimilarIncident
from sentinel.quality.incidents import Incident, IncidentStore


@pytest.fixture
def store(tmp_path, monkeypatch) -> IncidentStore:
    monkeypatch.setenv("SENTINEL_INCIDENTS_DB", str(tmp_path / "i.sqlite"))
    return IncidentStore()


@pytest.fixture
def manifest_path(tmp_path):
    p = tmp_path / "manifest.json"
    p.write_text(
        json.dumps(
            {
                "nodes": {
                    "model.sentinel.silver_trips_weather": {
                        "name": "silver_trips_weather",
                        "resource_type": "model",
                    },
                    "model.sentinel.fct_trips_daily": {
                        "name": "fct_trips_daily",
                        "resource_type": "model",
                    },
                },
                "parent_map": {
                    "model.sentinel.silver_trips_weather": [
                        "source.sentinel.bronze.tlc_yellow",
                        "source.sentinel.bronze.weather_nyc_daily",
                    ],
                    "model.sentinel.fct_trips_daily": [
                        "model.sentinel.silver_trips_weather",
                    ],
                },
                "child_map": {
                    "model.sentinel.silver_trips_weather": [
                        "model.sentinel.fct_trips_daily",
                    ],
                    "model.sentinel.fct_trips_daily": [],
                },
            }
        )
    )
    return p


def _insert_incident(store: IncidentStore, **overrides: Any) -> str:
    return store.insert(
        Incident(
            asset_key=overrides.pop("asset_key", "silver_trips_weather"),
            error_type=overrides.pop("error_type", "ChaosTriggered"),
            error_message=overrides.pop("error_message", "boom"),
            recent_metadata=overrides.pop(
                "recent_metadata",
                {"run_id": "run-1", "job_name": "j", "tags": {}},
            ),
            **overrides,
        )
    )


def test_build_raises_on_unknown_id(store):
    with pytest.raises(KeyError):
        context.build("does-not-exist", incident_store=store)


def test_build_minimal_no_extras(store, tmp_path):
    nid = _insert_incident(store)
    bundle = context.build(
        nid,
        incident_store=store,
        dbt_manifest_path=tmp_path / "missing-manifest.json",
        dagster_instance=None,
        incident_index=None,
    )
    assert bundle.incident["id"] == nid
    assert bundle.upstream == []
    assert bundle.downstream == []
    assert bundle.dbt_node is None
    assert bundle.recent_runs == []
    assert bundle.recent_logs == []
    assert bundle.similar_incidents == []


def test_build_resolves_dbt_lineage(store, manifest_path):
    nid = _insert_incident(store, asset_key="silver_trips_weather")
    bundle = context.build(
        nid,
        incident_store=store,
        dbt_manifest_path=manifest_path,
    )
    assert "source.sentinel.bronze.tlc_yellow" in bundle.upstream
    assert "model.sentinel.fct_trips_daily" in bundle.downstream
    assert bundle.dbt_node is not None
    assert bundle.dbt_node["name"] == "silver_trips_weather"


def test_build_handles_unknown_asset_key(store, manifest_path):
    nid = _insert_incident(store, asset_key="bronze.tlc_yellow")
    bundle = context.build(nid, incident_store=store, dbt_manifest_path=manifest_path)
    assert bundle.dbt_node is None
    assert bundle.upstream == []


def test_build_includes_run_history(store, tmp_path):
    nid = _insert_incident(store)

    fake_instance = MagicMock()
    rec = MagicMock()
    rec.dagster_run.run_id = "run-1"
    rec.dagster_run.status = "FAILURE"
    rec.dagster_run.tags = {"dagster/partition": "2024-04-01"}
    rec.dagster_run.asset_selection = set()
    rec.create_timestamp = 1234567890
    fake_instance.get_run_records.return_value = [rec]
    fake_instance.all_logs.return_value = [
        MagicMock(user_message="started"),
        MagicMock(user_message="failed: ChaosTriggered"),
    ]

    bundle = context.build(
        nid,
        incident_store=store,
        dbt_manifest_path=tmp_path / "missing.json",
        dagster_instance=fake_instance,
    )
    assert len(bundle.recent_runs) == 1
    assert bundle.recent_runs[0]["run_id"] == "run-1"
    assert "failed: ChaosTriggered" in bundle.recent_logs[-1]


def test_build_swallows_dagster_errors(store, tmp_path):
    nid = _insert_incident(store)
    bad_instance = MagicMock()
    bad_instance.get_run_records.side_effect = RuntimeError("dagster down")
    bad_instance.all_logs.side_effect = RuntimeError("logs down")

    bundle = context.build(
        nid,
        incident_store=store,
        dbt_manifest_path=tmp_path / "missing.json",
        dagster_instance=bad_instance,
    )
    assert bundle.recent_runs == []
    assert bundle.recent_logs == []


def test_build_includes_similar_incidents(store, tmp_path):
    nid = _insert_incident(store)

    fake_index = MagicMock(spec=IncidentIndex)
    fake_index.search.return_value = [
        SimilarIncident(
            incident_id="prev-1",
            score=0.91,
            asset_key="silver_trips_weather",
            error_type="ChaosTriggered",
            payload={"asset_key": "silver_trips_weather", "error_type": "ChaosTriggered"},
        )
    ]

    bundle = context.build(
        nid,
        incident_store=store,
        dbt_manifest_path=tmp_path / "missing.json",
        incident_index=fake_index,
    )
    assert len(bundle.similar_incidents) == 1
    assert bundle.similar_incidents[0]["incident_id"] == "prev-1"
    assert bundle.similar_incidents[0]["score"] == pytest.approx(0.91)


def test_build_qdrant_failure_is_swallowed(store, tmp_path):
    nid = _insert_incident(store)
    fake_index = MagicMock(spec=IncidentIndex)
    fake_index.search.side_effect = RuntimeError("qdrant unreachable")
    bundle = context.build(
        nid,
        incident_store=store,
        dbt_manifest_path=tmp_path / "missing.json",
        incident_index=fake_index,
    )
    assert bundle.similar_incidents == []
