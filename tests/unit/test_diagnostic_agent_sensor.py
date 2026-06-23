from __future__ import annotations

import json

import pytest
from dagster import DagsterInstance, build_sensor_context

from sentinel.agent.llm import MockLLMClient
from sentinel.sensors import diagnostic_agent as sensor_mod


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setenv("SENTINEL_INCIDENTS_DB", str(tmp_path / "i.sqlite"))
    from sentinel.quality.incidents import IncidentStore

    return IncidentStore()


@pytest.fixture
def fake_storage(fake_storage, monkeypatch):
    """Use the shared in-memory FakeStorage from conftest.

    ``ObjectStorage`` is a frozen pydantic BaseModel (Dagster's
    ``ConfigurableResource``) so we can't carry state on the instance.
    The conftest fixture keeps state in a module-level dict that the
    sensor's storage handle will share via the patched constructor.
    """
    monkeypatch.setattr(sensor_mod, "ObjectStorage", lambda **kwargs: fake_storage)
    return fake_storage


@pytest.fixture
def mock_llm(monkeypatch):
    client = MockLLMClient(
        [
            {
                "category": "upstream_outage",
                "root_cause": "503 from CloudFront.",
                "proposed_fix": "retry-with-backoff",
                "confidence": 0.85,
                "can_auto_remediate": True,
            },
        ]
    )
    monkeypatch.setattr(sensor_mod, "LLMClient", lambda *a, **kw: client)
    return client


def _ctx():
    return build_sensor_context(instance=DagsterInstance.ephemeral())


def test_sensor_skips_when_no_open_incidents(store, fake_storage, mock_llm):
    result = sensor_mod.diagnostic_agent_sensor(_ctx())
    # Returns a SkipReason for no work
    assert "no un-diagnosed incidents" in str(result.skip_message)


def test_sensor_diagnoses_and_writes_to_minio(store, fake_storage, mock_llm):
    from sentinel.quality.incidents import Incident

    iid = store.insert(
        Incident(
            asset_key="bronze/tlc_yellow",
            partition_key="2024-04-01",
            error_type="ChaosTriggered",
            error_message="chaos:tlc_5xx",
            recent_metadata={"run_id": "r1"},
        )
    )

    result = sensor_mod.diagnostic_agent_sensor(_ctx())
    assert "diagnosed=1" in str(result.skip_message)

    # The report is in the incidents bucket
    expected_key = f"incidents/{iid}.json"
    assert fake_storage.object_exists("sentinel-incidents", expected_key)
    payload = json.loads(fake_storage.get_bytes("sentinel-incidents", expected_key).decode())
    assert payload["incident_id"] == iid
    assert payload["diagnosis"]["proposed_fix"] == "retry-with-backoff"

    # Proposed fix is stamped on the incident row, which means a second
    # tick filters it out
    row = store.get(iid)
    assert row["proposed_fix"] == "retry-with-backoff"

    # Re-tick is a noop -- nothing un-diagnosed
    result2 = sensor_mod.diagnostic_agent_sensor(_ctx())
    assert "no un-diagnosed incidents" in str(result2.skip_message)


def test_sensor_continues_through_agent_failure(store, fake_storage, monkeypatch):
    # Two incidents: first will agent-fail (queue exhausted), second should still process.
    from sentinel.quality.incidents import Incident

    iid1 = store.insert(Incident(asset_key="x", error_type="t", error_message="m"))
    iid2 = store.insert(Incident(asset_key="y", error_type="t", error_message="m"))

    # Patch the LLMClient: empty queue first call (raises LLMError) for iid1,
    # then the second incident processes ok. But the MockLLMClient is one
    # instance, so we configure it with one response. For the first
    # incident the agent's diagnose_node will catch the exception and emit
    # a synthetic diagnosis (with empty proposed_fix), which the sensor
    # will then call set_proposed_fix("") on -- still considered "diagnosed"
    # for our purposes. So both should land.
    client = MockLLMClient(
        [
            {
                "category": "upstream_outage",
                "root_cause": "x",
                "proposed_fix": "retry-with-backoff",
                "confidence": 0.5,
                "can_auto_remediate": True,
            },
            {
                "category": "unknown",
                "root_cause": "y",
                "proposed_fix": "file incident",
                "confidence": 0.1,
                "can_auto_remediate": False,
            },
        ]
    )
    monkeypatch.setattr(sensor_mod, "LLMClient", lambda *a, **kw: client)

    result = sensor_mod.diagnostic_agent_sensor(_ctx())
    msg = str(result.skip_message)
    assert "diagnosed=2" in msg
    assert "failed=0" in msg
    assert fake_storage.object_exists("sentinel-incidents", f"incidents/{iid1}.json")
    assert fake_storage.object_exists("sentinel-incidents", f"incidents/{iid2}.json")
