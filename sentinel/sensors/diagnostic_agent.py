"""Sensor: on every new captured incident, fire the diagnostic agent.

Sibling to ``failure_capture_sensor`` (which writes the raw incident
row). This one reads ``status='open'`` incidents that don't yet have a
diagnosis, runs the agent, writes the formatted incident report to the
``sentinel-incidents`` MinIO bucket as JSON, and stores the proposed
fix back on the incident row.

Read-only with respect to the warehouse — the agent never mutates dbt
state or bronze parquet here. Week 10 wires the remediation executor
behind this same sensor by routing on ``can_auto_remediate``.

Why a separate sensor instead of doing this inside ``failure_capture``:

- Failure capture must be fast; a slow LLM call would slow the
  failure-event critical path and risk Dagster's sensor timeout.
- Decoupling means we can re-run the agent on a recorded incident
  without re-failing the asset. The eval suite in particular relies
  on this.
"""

from __future__ import annotations

import os
from typing import Any

from dagster import (
    DefaultSensorStatus,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from sentinel.agent.graph import AgentDeps, incident_report_to_json, run_agent
from sentinel.agent.llm import LLMClient
from sentinel.agent.remediation import RemediationDeps
from sentinel.chaos import state as chaos_state
from sentinel.observability.logging import get_logger
from sentinel.quality.incidents import IncidentStore
from sentinel.resources.storage import ObjectStorage
from sentinel.settings import get_settings

log = get_logger(__name__)


def _incidents_key(incident_id: str) -> str:
    # Flat layout; the bucket is small enough that prefixing by date
    # is premature. Revisit if we ever ship a real retention story.
    return f"incidents/{incident_id}.json"


def _build_deps(
    instance: Any | None = None,
    storage: ObjectStorage | None = None,
) -> AgentDeps:
    s = get_settings()
    return AgentDeps(
        llm=LLMClient(),
        incident_store=IncidentStore(),
        dagster_instance=instance,
        # incident_index left None at sensor startup; failing soft on
        # qdrant absence is intentional (see context._similar).
        incident_index=None,
        # Remediation runs *after* diagnosis, only when the model proposes an
        # allowlisted fix. We thread storage + chaos-state in here so the
        # action layer stays a pure function of its deps.
        remediation=RemediationDeps(
            storage=storage,
            chaos_state=chaos_state,
            bucket_bronze=s.bucket_bronze,
            bucket_incidents=s.bucket_incidents,
        ),
    )


@sensor(
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,  # opt-in; LLM calls cost money
    name="diagnostic_agent_sensor",
)
def diagnostic_agent_sensor(context: SensorEvaluationContext):
    """Pull open un-diagnosed incidents, run the agent, persist the report.

    Skipped from default-running for two reasons: (1) it makes outbound
    LLM calls, (2) it's the kind of sensor that should be opt-in for
    local development. Enable it explicitly from the Dagster UI when
    you want the agent loop active.
    """
    settings = get_settings()
    store = IncidentStore()
    open_rows = store.list_open(limit=10)
    targets = [r for r in open_rows if not r.get("proposed_fix")]
    if not targets:
        return SkipReason("no un-diagnosed incidents")

    storage = ObjectStorage(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )
    storage.ensure_bucket(settings.bucket_incidents)
    storage.ensure_bucket(settings.bucket_bronze)

    deps = _build_deps(instance=context.instance, storage=storage)
    diagnosed: list[str] = []
    failed: list[str] = []
    auto_remediated: list[str] = []
    for row in targets:
        incident_id = row["id"]
        try:
            report = run_agent(deps, incident_id=incident_id)
        except Exception as e:  # capture broadly: an agent failure is itself signal
            log.error("agent.sensor.run_failed", incident_id=incident_id, err=str(e))
            failed.append(incident_id)
            continue

        # Persist the report to MinIO and stash the proposed fix on the row.
        payload = incident_report_to_json(report)
        storage.put_bytes(
            settings.bucket_incidents,
            _incidents_key(incident_id),
            payload,
            content_type="application/json",
        )
        store.set_proposed_fix(
            incident_id,
            proposed_fix=(report.get("diagnosis") or {}).get("proposed_fix", ""),
        )
        diagnosed.append(incident_id)
        action = report.get("proposed_action") or {}
        if action.get("status") == "executed":
            auto_remediated.append(incident_id)

    summary = (
        f"diagnosed={len(diagnosed)} auto_remediated={len(auto_remediated)} failed={len(failed)}"
    )
    log.info(
        "agent.sensor.tick",
        diagnosed=diagnosed,
        auto_remediated=auto_remediated,
        failed=failed,
    )

    # The remediator emits its RunRequest list separately (next-run specs
    # are inside each incident JSON). For now we surface them in the
    # SkipReason; week 11 wires a follow-up sensor that drains pending
    # next_run entries into actual RunRequests.
    if not diagnosed and not failed:
        return SkipReason("no work this tick")
    return SkipReason(summary)


# Optional manual entry point so an operator can drop into a python repl,
# replay one incident through the agent, and dump the report. Mostly used
# by the eval harness; not wired to a CLI subcommand yet.
def run_for_incident(incident_id: str) -> dict[str, Any]:
    if not os.environ.get("SENTINEL_LLM_MODEL") and os.environ.get("SENTINEL_AGENT_REQUIRE_MODEL"):
        raise RuntimeError("SENTINEL_LLM_MODEL not set; refusing to run agent in 'require' mode")
    deps = _build_deps()
    return run_agent(deps, incident_id=incident_id)
