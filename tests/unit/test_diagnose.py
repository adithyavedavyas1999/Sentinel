from __future__ import annotations

import pytest

from sentinel.agent import diagnose as diag
from sentinel.agent.llm import MockLLMClient


@pytest.fixture
def incident() -> dict:
    return {
        "id": "inc-1",
        "asset_key": "bronze/tlc_yellow",
        "partition_key": "2024-04-01",
        "error_type": "ChaosTriggered",
        "error_message": "chaos:tlc_5xx -- simulated upstream 5xx",
    }


@pytest.fixture
def context_payload() -> dict:
    return {
        "upstream": [],
        "downstream": ["model.sentinel.stg_tlc_yellow"],
        "recent_runs": [{"run_id": "r1", "status": "FAILURE"}],
        "recent_logs": ["fetch started", "503 Server Error"],
        "similar_incidents": [
            {
                "asset_key": "bronze/tlc_yellow",
                "error_type": "ChaosTriggered",
                "score": 0.92,
                "payload": {"proposed_fix": "retry-with-backoff"},
            }
        ],
    }


def test_diagnose_returns_parsed(incident, context_payload):
    llm = MockLLMClient(
        [
            {
                "category": "upstream_outage",
                "root_cause": "TLC CloudFront returned 503; transient.",
                "proposed_fix": "retry-with-backoff",
                "confidence": 0.84,
                "can_auto_remediate": True,
            }
        ]
    )
    parsed, raw = diag.diagnose(
        llm=llm, incident=incident, context_payload=context_payload, category_hint="upstream_outage"
    )
    assert parsed.category == "upstream_outage"
    assert parsed.proposed_fix == "retry-with-backoff"
    assert parsed.can_auto_remediate is True
    assert raw.model == "mock"


def test_validate_remediation_strips_off_allowlist():
    bad = diag.Diagnosis(
        category="dbt_error",
        root_cause="bad SQL",
        proposed_fix="rewrite-the-model",  # not on allowlist
        confidence=0.7,
        can_auto_remediate=True,
    )
    validated = diag.validate_remediation_claim(bad)
    assert validated.can_auto_remediate is False
    # everything else stays
    assert validated.proposed_fix == "rewrite-the-model"
    assert validated.category == "dbt_error"


def test_validate_remediation_keeps_when_on_allowlist():
    good = diag.Diagnosis(
        category="schema_drift",
        root_cause="x",
        proposed_fix="coerce-to-string",
        confidence=0.6,
        can_auto_remediate=True,
    )
    assert diag.validate_remediation_claim(good).can_auto_remediate is True


def test_validate_remediation_idempotent_when_false():
    nope = diag.Diagnosis(
        category="unknown",
        root_cause="x",
        proposed_fix="ask-a-human",
        confidence=0.2,
        can_auto_remediate=False,
    )
    assert diag.validate_remediation_claim(nope).can_auto_remediate is False


def test_prompt_includes_classifier_hint(incident, context_payload):
    # We can't easily assert on the prompt sent to the mock without monkeypatching,
    # but the MockLLMClient records all calls. Use that.
    llm = MockLLMClient(
        [
            {
                "category": "upstream_outage",
                "root_cause": "x",
                "proposed_fix": "retry-with-backoff",
                "confidence": 0.5,
                "can_auto_remediate": True,
            }
        ]
    )
    diag.diagnose(
        llm=llm, incident=incident, context_payload=context_payload, category_hint="upstream_outage"
    )
    [call] = llm.calls
    assert "Heuristic category guess (you may override): upstream_outage" in call["prompt"]
    assert "bronze/tlc_yellow" in call["prompt"]


def test_prompt_handles_empty_similar(incident, context_payload):
    context_payload["similar_incidents"] = []
    llm = MockLLMClient(
        [
            {
                "category": "unknown",
                "root_cause": "x",
                "proposed_fix": "investigate",
                "confidence": 0.1,
                "can_auto_remediate": False,
            }
        ]
    )
    parsed, _ = diag.diagnose(
        llm=llm, incident=incident, context_payload=context_payload, category_hint=None
    )
    assert parsed.category == "unknown"
    [call] = llm.calls
    assert "Heuristic category guess" not in call["prompt"]
    assert "(none)" in call["prompt"]
