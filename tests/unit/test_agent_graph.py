from __future__ import annotations

import json
from typing import Any

import pytest

from sentinel.agent.graph import (
    AgentDeps,
    _classify,
    incident_report_to_json,
    run_agent,
)
from sentinel.agent.llm import MockLLMClient
from sentinel.quality.incidents import Incident, IncidentStore


@pytest.fixture
def store(tmp_path, monkeypatch) -> IncidentStore:
    monkeypatch.setenv("SENTINEL_INCIDENTS_DB", str(tmp_path / "i.sqlite"))
    return IncidentStore()


def _insert(store: IncidentStore, **overrides: Any) -> str:
    return store.insert(
        Incident(
            asset_key=overrides.pop("asset_key", "bronze/tlc_yellow"),
            partition_key=overrides.pop("partition_key", "2024-04-01"),
            error_type=overrides.pop("error_type", "ChaosTriggered"),
            error_message=overrides.pop("error_message", "chaos:tlc_5xx -- simulated 5xx"),
            recent_metadata=overrides.pop("recent_metadata", {"run_id": "r1"}),
            **overrides,
        )
    )


# --- _classify --------------------------------------------------------------


def test_classify_chaos_tlc_5xx_is_upstream_outage():
    assert (
        _classify(
            {
                "error_type": "ChaosTriggered",
                "error_message": "chaos:tlc_5xx",
                "asset_key": "bronze/tlc_yellow",
            }
        )
        == "upstream_outage"
    )


def test_classify_chaos_duckdb_lock_is_infra():
    assert (
        _classify(
            {
                "error_type": "ChaosTriggered",
                "error_message": "chaos:duckdb_lock -- simulated lock",
                "asset_key": "silver/x",
            }
        )
        == "infra"
    )


def test_classify_dbt_error():
    assert (
        _classify(
            {
                "error_type": "DbtRuntimeError",
                "error_message": "compile failure",
                "asset_key": "silver/x",
            }
        )
        == "dbt_error"
    )


def test_classify_unknown_when_no_rule_matches():
    assert (
        _classify({"error_type": "RandomFoo", "error_message": "x", "asset_key": "y"}) == "unknown"
    )


# --- full graph -------------------------------------------------------------


def _diagnosis_response(**overrides: Any) -> dict[str, Any]:
    base = {
        "category": "upstream_outage",
        "root_cause": "TLC returned 503 -- transient.",
        "proposed_fix": "retry-with-backoff",
        "confidence": 0.82,
        "can_auto_remediate": True,
    }
    base.update(overrides)
    return base


def test_run_agent_happy_path(store):
    nid = _insert(store)
    llm = MockLLMClient([_diagnosis_response()])
    deps = AgentDeps(llm=llm, incident_store=store)

    report = run_agent(deps, incident_id=nid)

    assert report["incident_id"] == nid
    assert report["asset_key"] == "bronze/tlc_yellow"
    assert report["diagnosis"]["category"] == "upstream_outage"
    assert report["diagnosis"]["proposed_fix"] == "retry-with-backoff"
    assert report["diagnosis"]["can_auto_remediate"] is True
    assert report["category_hint"] == "upstream_outage"
    assert report["generated_at"]
    assert report["errors"] == []


def test_run_agent_strips_off_allowlist_remediation_claim(store):
    nid = _insert(store, error_type="DbtRuntimeError", error_message="compile failure")
    # model lies: claims auto-remediation for an off-allowlist fix
    llm = MockLLMClient(
        [
            _diagnosis_response(
                category="dbt_error",
                proposed_fix="rewrite-the-model",
                can_auto_remediate=True,
            )
        ]
    )
    deps = AgentDeps(llm=llm, incident_store=store)
    report = run_agent(deps, incident_id=nid)
    assert report["diagnosis"]["proposed_fix"] == "rewrite-the-model"
    # graph validates: forces back to false
    assert report["diagnosis"]["can_auto_remediate"] is False


def test_run_agent_normalizes_unknown_category(store):
    nid = _insert(store)
    llm = MockLLMClient([_diagnosis_response(category="aliens", confidence=0.1)])
    deps = AgentDeps(llm=llm, incident_store=store)
    report = run_agent(deps, incident_id=nid)
    assert report["diagnosis"]["category"] == "unknown"


def test_run_agent_falls_back_when_llm_raises(store):
    nid = _insert(store)
    # Empty queue -> MockLLMClient raises LLMError on first complete()
    llm = MockLLMClient([])
    deps = AgentDeps(llm=llm, incident_store=store)
    report = run_agent(deps, incident_id=nid)
    # Fallback diagnosis stamped with confidence=0 and can_auto_remediate=False
    assert report["diagnosis"]["confidence"] == 0.0
    assert report["diagnosis"]["can_auto_remediate"] is False
    assert "agent diagnose failed" in report["diagnosis"]["root_cause"]
    # And the error is captured on the report
    assert report["errors"]


def test_run_agent_unknown_incident_returns_report_with_errors(store):
    """Missing incident -> graph still completes, but ``errors`` is populated
    and the diagnosis is the synthetic fallback. Callers gate on ``errors``."""
    llm = MockLLMClient([])
    deps = AgentDeps(llm=llm, incident_store=store)
    report = run_agent(deps, incident_id="does-not-exist")
    assert report["incident_id"] is None
    assert report["errors"]
    assert any("does-not-exist" in e for e in report["errors"])


def test_incident_report_to_json_is_stable():
    a = {"x": 1, "y": [1, 2]}
    b = {"y": [1, 2], "x": 1}
    # sort_keys=True means equivalent dicts serialize identically.
    assert incident_report_to_json(a) == incident_report_to_json(b)
    # And is valid JSON.
    assert json.loads(incident_report_to_json(a)) == {"x": 1, "y": [1, 2]}


# --- propose_action through the graph ---------------------------------------


def test_run_agent_proposes_retry_action_when_remediation_wired(store):
    """End-to-end: chaos:tlc_5xx incident -> agent diagnoses retry-with-backoff,
    propose_action node calls the remediation, ``proposed_action`` is stamped
    on the report. Exercises both the new graph node and the registry."""
    from sentinel.agent.remediation import RemediationDeps
    from tests.unit.test_remediation import StubChaosState

    nid = _insert(store)
    chaos = StubChaosState()
    chaos.active.add("tlc_5xx")
    llm = MockLLMClient([_diagnosis_response()])
    deps = AgentDeps(
        llm=llm,
        incident_store=store,
        remediation=RemediationDeps(chaos_state=chaos),
    )

    report = run_agent(deps, incident_id=nid)
    action = report["proposed_action"]
    assert action["status"] == "executed"
    assert action["action"] == "retry-with-backoff"
    assert action["next_run"]["partition_key"] == "2024-04-01"
    assert action["next_run"]["run_tags"]["sentinel.cleared_flag"] == "tlc_5xx"
    assert "tlc_5xx" not in chaos.active


def test_run_agent_proposed_action_skipped_when_no_remediation_deps(store):
    """No remediation deps -> propose_action skips, graph still completes."""
    nid = _insert(store)
    llm = MockLLMClient([_diagnosis_response()])
    deps = AgentDeps(llm=llm, incident_store=store)  # remediation=None
    report = run_agent(deps, incident_id=nid)
    assert report["proposed_action"]["status"] == "skipped"
    assert "no remediation deps wired" in report["proposed_action"]["reason"]


def test_run_agent_proposed_action_skipped_when_off_allowlist(store):
    """Model proposes an off-allowlist fix -> graph stamps skipped with reason."""
    from sentinel.agent.remediation import RemediationDeps

    nid = _insert(store)
    llm = MockLLMClient(
        [_diagnosis_response(proposed_fix="rewrite-the-model", can_auto_remediate=True)]
    )
    deps = AgentDeps(
        llm=llm,
        incident_store=store,
        remediation=RemediationDeps(),
    )
    report = run_agent(deps, incident_id=nid)
    # validate_remediation_claim flipped can_auto_remediate to False,
    # so propose_action sees the disabled flag first.
    assert report["proposed_action"]["status"] == "skipped"
    assert report["diagnosis"]["can_auto_remediate"] is False


def test_run_agent_proposed_action_skipped_when_guard_fails(store):
    """Off-asset incident -> retry guard rejects -> proposed_action.skipped."""
    from sentinel.agent.remediation import RemediationDeps
    from tests.unit.test_remediation import StubChaosState

    nid = _insert(
        store,
        asset_key="silver/trips_weather",
        error_type="HTTPStatusError",
        error_message="503",
    )
    llm = MockLLMClient([_diagnosis_response()])
    deps = AgentDeps(
        llm=llm,
        incident_store=store,
        remediation=RemediationDeps(chaos_state=StubChaosState()),
    )
    report = run_agent(deps, incident_id=nid)
    assert report["proposed_action"]["status"] == "skipped"
    assert report["proposed_action"]["reason"] == "guard rejected"


def test_run_agent_proposed_action_failure_when_execute_raises(store, monkeypatch):
    """Action.execute() raising -> propose_action.failed with error captured."""
    from sentinel.agent.remediation import _REGISTRY, RemediationDeps

    class Boom:
        name = "retry-with-backoff"

        def guard(self, incident):
            return True

        def execute(self, incident, deps):
            raise RuntimeError("kaboom")

        def rollback(self, result, deps):
            pass

    monkeypatch.setitem(_REGISTRY, "retry-with-backoff", Boom())
    nid = _insert(store)
    llm = MockLLMClient([_diagnosis_response()])
    deps = AgentDeps(llm=llm, incident_store=store, remediation=RemediationDeps())
    report = run_agent(deps, incident_id=nid)
    assert report["proposed_action"]["status"] == "failed"
    assert "kaboom" in report["proposed_action"]["reason"]
    assert any("kaboom" in e for e in report["errors"])
