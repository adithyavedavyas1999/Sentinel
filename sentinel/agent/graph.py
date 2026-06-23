"""LangGraph state machine for the diagnostic agent.

Four nodes, one happy path. The graph exists for two reasons:

1. **Auditability.** Each node writes its output into state under a
   distinct key, so the resulting incident report is reproducible from
   the recorded intermediate values. The agent eval suite (week 10+)
   replays states; it does not re-call the LLM.

2. **Refactor surface.** The week-9 shape almost certainly isn't the
   final one — ADR-004's allowlisted remediation lands in week 10 and
   will want to split ``diagnose`` into ``classify`` + ``propose_action``.
   LangGraph lets us split a node without rewiring callers.

We intentionally use LangGraph only for orchestration. LLM calls go
through ``sentinel.agent.llm`` (which fronts LiteLLM) so we never
inherit LangChain's provider quirks. See ADR-005.

Read-only at this stage. ``can_auto_remediate=true`` in the diagnosis is
just metadata; nothing here mutates pipeline state. Week 10 wires the
remediator after this node.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from sentinel.agent.context import IncidentContext
from sentinel.agent.context import build as build_context
from sentinel.agent.diagnose import (
    CATEGORIES,
    Diagnosis,
    validate_remediation_claim,
)
from sentinel.agent.diagnose import (
    diagnose as diagnose_step,
)
from sentinel.agent.embeddings import IncidentIndex
from sentinel.agent.llm import LLMClientProtocol
from sentinel.agent.remediation import (
    RemediationDeps,
)
from sentinel.agent.remediation import (
    allowlist as remediation_allowlist,
)
from sentinel.agent.remediation import (
    get_action as get_remediation_action,
)
from sentinel.observability.logging import get_logger
from sentinel.quality.incidents import IncidentStore

log = get_logger(__name__)


class AgentState(TypedDict, total=False):
    """LangGraph state passed between nodes.

    ``total=False`` so nodes can populate keys incrementally without
    forcing every test fixture to fill all fields.
    """

    incident_id: str
    incident: dict[str, Any]
    category_hint: str
    context: dict[str, Any]
    diagnosis: dict[str, Any]
    proposed_action: dict[str, Any]
    incident_report: dict[str, Any]
    errors: list[str]


@dataclass
class AgentDeps:
    """Read-only dependencies injected into the graph.

    Keeping deps in a frozen-ish dataclass means tests can build a graph
    with mocks (``MockLLMClient``, in-memory ``IncidentStore``, no Dagster
    instance) without touching globals.

    ``remediation`` is optional: when omitted, the propose-action node
    returns ``proposed_action={"status": "skipped"}`` and the graph
    behaves like the week-9 read-only build. Wire it in (sensor does so
    by default) when the remediator should run alongside diagnosis.
    """

    llm: LLMClientProtocol
    incident_store: IncidentStore
    incident_index: IncidentIndex | None = None
    dagster_instance: Any | None = None
    remediation: RemediationDeps | None = None
    similar_top_k: int = 5
    log_lines_limit: int = 200


# --- nodes ------------------------------------------------------------------


# Map (error_type, asset_key_substring) -> category. Order matters; first
# match wins. Substrings are case-insensitive on asset_key. The model can
# override this in its JSON response; we just hand it as a hint.
_CLASSIFIER_RULES: tuple[tuple[str, str, str], ...] = (
    # (error_type substring, asset_key substring, category)
    ("ChaosTriggered", "tlc_5xx", "upstream_outage"),
    ("ChaosTriggered", "weather_429", "upstream_outage"),
    ("ChaosTriggered", "late_partition", "upstream_outage"),
    ("ChaosTriggered", "duckdb_lock", "infra"),
    ("ChaosTriggered", "schema", "schema_drift"),
    ("HTTPStatusError", "", "upstream_outage"),
    ("TimeoutException", "", "upstream_outage"),
    ("ConnectError", "", "upstream_outage"),
    ("DbtRuntimeError", "", "dbt_error"),
    ("DagsterDbtCliRuntimeError", "", "dbt_error"),
    ("AssertionError", "stg_", "data_quality"),
    ("ValueError", "weather", "schema_drift"),
)


def _classify(incident: dict[str, Any]) -> str:
    error_type = incident.get("error_type", "") or ""
    error_msg = (incident.get("error_message", "") or "").lower()
    asset_key = (incident.get("asset_key", "") or "").lower()
    for err_sub, asset_sub, category in _CLASSIFIER_RULES:
        if err_sub.lower() in error_type.lower() and (
            not asset_sub or asset_sub.lower() in asset_key or asset_sub.lower() in error_msg
        ):
            return category
    return "unknown"


def classify_node(state: AgentState, deps: AgentDeps) -> AgentState:
    incident = state.get("incident") or deps.incident_store.get(state["incident_id"])
    if incident is None:
        return {"errors": [*state.get("errors", []), f"incident not found: {state['incident_id']}"]}
    hint = _classify(incident)
    log.info("agent.classify", incident_id=incident.get("id"), hint=hint)
    return {"incident": incident, "category_hint": hint}


def gather_context_node(state: AgentState, deps: AgentDeps) -> AgentState:
    if "incident" not in state:
        return state
    try:
        ctx: IncidentContext = build_context(
            state["incident"]["id"],
            incident_store=deps.incident_store,
            dagster_instance=deps.dagster_instance,
            incident_index=deps.incident_index,
            log_lines_limit=deps.log_lines_limit,
            similar_top_k=deps.similar_top_k,
        )
    except KeyError as e:
        # incident dropped from sqlite between classify and now -- rare,
        # but a Dagster restart could do it. fail loud.
        log.error("agent.gather_context.missing_incident", err=str(e))
        return {"errors": [*state.get("errors", []), str(e)]}
    return {"context": ctx.to_dict()}


def diagnose_node(state: AgentState, deps: AgentDeps) -> AgentState:
    if "incident" not in state or "context" not in state:
        return state
    try:
        parsed, raw = diagnose_step(
            llm=deps.llm,
            incident=state["incident"],
            context_payload=state["context"],
            category_hint=state.get("category_hint"),
        )
    except Exception as e:  # LLMError, provider exceptions, anything
        log.error("agent.diagnose.failed", err=str(e))
        # Fall back to a synthetic diagnosis so the downstream sensor still
        # gets a useful incident JSON. We mark confidence=0 so anything
        # consuming it knows this isn't a real model output.
        fallback = Diagnosis(
            category=state.get("category_hint", "unknown"),
            root_cause=f"agent diagnose failed: {e!s}",
            proposed_fix="file incident for human review",
            confidence=0.0,
            can_auto_remediate=False,
        )
        return {"diagnosis": fallback.model_dump(), "errors": [*state.get("errors", []), str(e)]}

    # Defensive: the model is allowed to claim categories outside our
    # canonical list. Normalize unknown values to "unknown".
    if parsed.category not in CATEGORIES:
        parsed = parsed.model_copy(update={"category": "unknown"})

    parsed = validate_remediation_claim(parsed)
    log.info(
        "agent.diagnose.ok",
        category=parsed.category,
        confidence=parsed.confidence,
        can_auto_remediate=parsed.can_auto_remediate,
        tokens_in=raw.tokens_in,
        tokens_out=raw.tokens_out,
    )
    return {"diagnosis": parsed.model_dump()}


def propose_action_node(state: AgentState, deps: AgentDeps) -> AgentState:
    """Translate the diagnosis's ``proposed_fix`` into a concrete action plan.

    Off-allowlist fixes, no-deps, and guard failures all collapse to
    ``status='skipped'`` with a reason so the format-incident node has
    something deterministic to render.
    """
    diagnosis = state.get("diagnosis") or {}
    proposed_fix = diagnosis.get("proposed_fix", "")
    can_auto = bool(diagnosis.get("can_auto_remediate", False))

    if not can_auto:
        return {
            "proposed_action": {
                "status": "skipped",
                "reason": "diagnosis.can_auto_remediate=false",
            }
        }
    if proposed_fix not in remediation_allowlist():
        return {
            "proposed_action": {
                "status": "skipped",
                "reason": f"proposed_fix '{proposed_fix}' not on allowlist",
            }
        }
    if deps.remediation is None:
        return {
            "proposed_action": {
                "status": "skipped",
                "reason": "no remediation deps wired",
            }
        }

    action = get_remediation_action(proposed_fix)
    incident = state.get("incident") or {}
    if action is None or not action.guard(incident):
        return {
            "proposed_action": {
                "status": "skipped",
                "reason": "guard rejected",
                "action": proposed_fix,
            }
        }

    try:
        result = action.execute(incident, deps.remediation)
    except Exception as e:
        log.exception("agent.propose_action.execute_failed", action=action.name)
        return {
            "proposed_action": {
                "status": "failed",
                "reason": str(e),
                "action": action.name,
            },
            "errors": [*state.get("errors", []), f"remediation {action.name}: {e!s}"],
        }

    next_run = result.next_run
    return {
        "proposed_action": {
            "status": "executed" if result.success else "execute_returned_failure",
            "action": result.action,
            "description": result.description,
            "next_run": (
                {
                    "asset_key": next_run.asset_key,
                    "partition_key": next_run.partition_key,
                    "run_tags": next_run.run_tags,
                }
                if next_run
                else None
            ),
            "rollback_data": result.rollback_data,
        }
    }


def format_incident_node(state: AgentState, deps: AgentDeps) -> AgentState:
    incident = state.get("incident") or {}
    diagnosis = state.get("diagnosis") or {}
    context = state.get("context") or {}
    report = {
        "incident_id": incident.get("id"),
        "asset_key": incident.get("asset_key"),
        "partition_key": incident.get("partition_key"),
        "error_type": incident.get("error_type"),
        "error_message": incident.get("error_message"),
        "category_hint": state.get("category_hint"),
        "diagnosis": diagnosis,
        "proposed_action": state.get("proposed_action") or {"status": "skipped"},
        "context_summary": {
            "upstream": context.get("upstream", []),
            "downstream": context.get("downstream", []),
            "recent_run_count": len(context.get("recent_runs", [])),
            "similar_incident_count": len(context.get("similar_incidents", [])),
        },
        "generated_at": datetime.now(UTC).isoformat(),
        "errors": state.get("errors", []),
    }
    log.info("agent.format_incident.ok", incident_id=incident.get("id"))
    return {"incident_report": report}


# --- assembly ---------------------------------------------------------------


def build_graph(deps: AgentDeps):
    """Compile the LangGraph state machine. Each node is wrapped to inject
    ``deps`` since LangGraph nodes are functions of (state,) only.
    """
    g = StateGraph(AgentState)

    g.add_node("classify", lambda s: classify_node(s, deps))
    g.add_node("gather_context", lambda s: gather_context_node(s, deps))
    g.add_node("diagnose", lambda s: diagnose_node(s, deps))
    g.add_node("propose_action", lambda s: propose_action_node(s, deps))
    g.add_node("format_incident", lambda s: format_incident_node(s, deps))

    g.set_entry_point("classify")
    g.add_edge("classify", "gather_context")
    g.add_edge("gather_context", "diagnose")
    g.add_edge("diagnose", "propose_action")
    g.add_edge("propose_action", "format_incident")
    g.add_edge("format_incident", END)

    return g.compile()


def run_agent(deps: AgentDeps, *, incident_id: str) -> dict[str, Any]:
    """Run the full graph for one incident, return the formatted report.

    Single entry point for the sensor and for tests. The graph itself is
    deterministic given (incident_store contents, llm mock contents);
    that's the property the eval suite relies on.

    The format-incident node always runs, so this never returns None.
    When earlier nodes fail (incident missing, LLM down), the report's
    ``errors`` list is populated and the diagnosis is a low-confidence
    fallback. Callers should treat ``errors`` as the failure signal.
    """
    app = build_graph(deps)
    final: AgentState = app.invoke({"incident_id": incident_id})
    # format_incident_node always populates this; the assignment satisfies mypy.
    return final["incident_report"]  # type: ignore[typeddict-item]


def incident_report_to_json(report: dict[str, Any]) -> bytes:
    """Stable JSON serialization for the MinIO `incidents/` bucket."""
    return json.dumps(report, indent=2, default=str, sort_keys=True).encode("utf-8")
