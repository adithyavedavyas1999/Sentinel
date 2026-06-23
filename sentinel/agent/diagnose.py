"""Diagnosis prompt + schema for the agent's middle step.

The diagnose node is the only LLM call in the read-only loop (week 9).
Everything else (classify, gather context, format) is deterministic
Python so we can test the failure modes without consuming API tokens.

The prompt is the one that survived three rounds of iteration in
``docs/notebooks/prompt_iteration.ipynb``. Two key constraints baked in:

1. JSON mode + pydantic schema. The model is allowed to be wrong; it
   is not allowed to be unparseable.
2. ``can_auto_remediate`` defaults false. The model has to actively
   match one of the allowlisted fixes (week 10) to set it true, and
   the graph still re-validates against the allowlist before any
   action runs.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from sentinel.agent.llm import LLMClientProtocol, LLMResponse
from sentinel.observability.logging import get_logger

log = get_logger(__name__)

# Categories the model is allowed to pick. Kept short on purpose; widening
# this is a prompt-iteration decision, not a runtime config one.
CATEGORIES = (
    "upstream_outage",
    "schema_drift",
    "data_quality",
    "infra",
    "dbt_error",
    "unknown",
)

# Allowlisted proposed-fix strings. The agent must use these exact tokens
# if it wants ``can_auto_remediate=true`` to survive the post-validate
# step in ``sentinel.agent.graph``.
ALLOWLISTED_FIXES = frozenset(
    {
        "retry-with-backoff",
        "partition-window-slip",
        "coerce-to-string",
    }
)


class Diagnosis(BaseModel):
    """Structured diagnosis the model is required to return.

    The fields mirror what a human on-call would put into a ticket:
    one-line root cause, one-line fix, a confidence number we don't
    fully trust but log anyway, and an explicit flag for whether the
    fix is on the auto-remediation allowlist.
    """

    category: str = Field(description=f"one of: {', '.join(CATEGORIES)}")
    root_cause: str = Field(description="one sentence, plain English")
    proposed_fix: str = Field(
        description=(
            "If can_auto_remediate=true, this MUST be one of: "
            f"{', '.join(sorted(ALLOWLISTED_FIXES))}. "
            "Otherwise a short free-text description of the manual fix."
        )
    )
    confidence: float = Field(ge=0.0, le=1.0)
    can_auto_remediate: bool = Field(
        description=(
            "True only when proposed_fix is on the allowlist AND you are "
            "confident the action is safe. Default to false when unsure."
        ),
    )


_SYSTEM_PROMPT = (
    "You triage data pipeline failures for a self-healing pipeline.\n"
    "Read the incident record + context bundle and return a single JSON\n"
    "object matching the Diagnosis schema.\n"
    "\n"
    "Rules:\n"
    "- Be terse. One sentence per free-text field.\n"
    "- If you are not sure, set category=unknown and confidence < 0.5.\n"
    "- can_auto_remediate may be true ONLY when proposed_fix is one of:\n"
    "  retry-with-backoff, partition-window-slip, coerce-to-string.\n"
    "- Never invent log lines, partition keys, or asset names that are\n"
    "  not present in the context.\n"
    "- Return JSON only. Do not wrap in markdown code fences."
)


def diagnose(
    *,
    llm: LLMClientProtocol,
    incident: dict[str, Any],
    context_payload: dict[str, Any],
    category_hint: str | None = None,
) -> tuple[Diagnosis, LLMResponse]:
    """Run the diagnose prompt and return (parsed, raw response).

    ``category_hint`` comes from the classifier node; the model is free
    to override it (a heuristic can be wrong) but we surface it in the
    prompt so the model has somewhere to start.
    """
    prompt = _build_prompt(
        incident=incident,
        context_payload=context_payload,
        category_hint=category_hint,
    )
    resp = llm.complete(prompt, system=_SYSTEM_PROMPT, json_schema=Diagnosis)
    assert resp.parsed is not None  # json_schema set -> parsed populated
    parsed = Diagnosis.model_validate(resp.parsed)
    return parsed, resp


def _build_prompt(
    *,
    incident: dict[str, Any],
    context_payload: dict[str, Any],
    category_hint: str | None,
) -> str:
    hint_line = (
        f"Heuristic category guess (you may override): {category_hint}\n" if category_hint else ""
    )
    return (
        f"{hint_line}"
        "INCIDENT\n"
        f"asset_key: {incident.get('asset_key', '')}\n"
        f"partition_key: {incident.get('partition_key', '')}\n"
        f"error_type: {incident.get('error_type', '')}\n"
        f"error_message: {incident.get('error_message', '')}\n"
        "\n"
        "CONTEXT\n"
        f"upstream: {context_payload.get('upstream', [])}\n"
        f"downstream: {context_payload.get('downstream', [])}\n"
        f"recent_runs: {len(context_payload.get('recent_runs', []))} entries\n"
        "recent_log_excerpts (tail):\n"
        + "\n".join(f"  {line}" for line in (context_payload.get("recent_logs") or [])[-10:])
        + "\n\n"
        "SIMILAR PAST INCIDENTS\n"
        + _format_similar(context_payload.get("similar_incidents", []))
        + "\n"
        "Return JSON only."
    )


def _format_similar(similar: list[dict[str, Any]]) -> str:
    if not similar:
        return "(none)\n"
    out = []
    for s in similar[:3]:  # top-3 is plenty; more dilutes the signal
        out.append(
            f"- asset={s.get('asset_key', '')} "
            f"error_type={s.get('error_type', '')} "
            f"score={s.get('score', 0):.2f} "
            f"prior_fix={(s.get('payload') or {}).get('proposed_fix', '')}"
        )
    return "\n".join(out) + "\n"


def validate_remediation_claim(diagnosis: Diagnosis) -> Diagnosis:
    """Strip ``can_auto_remediate=true`` if the fix isn't on the allowlist.

    The model occasionally claims auto-remediate for proposed fixes
    outside the allowlist (e.g. ``rerun-dbt``). We re-check on our side
    so the week-10 executor never has to take the model's word for it.
    """
    if not diagnosis.can_auto_remediate:
        return diagnosis
    if diagnosis.proposed_fix not in ALLOWLISTED_FIXES:
        log.warning(
            "agent.diagnose.remediation_off_allowlist",
            proposed_fix=diagnosis.proposed_fix,
        )
        return diagnosis.model_copy(update={"can_auto_remediate": False})
    return diagnosis
