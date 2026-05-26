"""Sensor that captures asset failures as structured incidents.

Fires on any RunFailure event. Walks the failed run's records, builds an
Incident, persists it. The Phase 2 agent reads these.
"""
from __future__ import annotations

import traceback

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunStatusSensorContext,
    SkipReason,
    run_status_sensor,
)

from sentinel.observability.metrics import dq_check_total
from sentinel.quality.incidents import Incident, IncidentStore


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=10,
)
def failure_capture_sensor(context: RunStatusSensorContext):
    run = context.dagster_run
    instance = context.instance
    records = instance.all_logs(run.run_id, of_type=None)

    # Pull asset_key + error info from the records. dagster's log structure
    # is verbose; we grab what we need and move on.
    asset_key = None
    partition_key = run.tags.get("dagster/partition")
    error_type = "Unknown"
    error_message = ""
    stack = ""

    for r in records:
        de = r.dagster_event
        if de is None:
            continue
        if de.is_failure and de.event_specific_data is not None:
            err = getattr(de.event_specific_data, "error", None)
            if err is not None:
                error_type = err.cls_name or error_type
                error_message = err.message or error_message
                stack = "".join(err.stack or []) or traceback.format_exc()
        ak = getattr(de, "asset_key", None)
        if ak is not None:
            asset_key = ak.to_user_string()

    if asset_key is None:
        return SkipReason("no asset_key on failure event")

    store = IncidentStore()
    incident = Incident(
        asset_key=asset_key,
        partition_key=partition_key,
        error_type=error_type,
        error_message=error_message,
        stack_trace=stack,
        recent_metadata={
            "run_id": run.run_id,
            "job_name": run.job_name,
            "tags": dict(run.tags),
        },
    )
    incident_id = store.insert(incident)
    dq_check_total.labels(check_name="asset_failure", result="fail").inc()
    context.log.info(f"captured incident {incident_id} for asset {asset_key}")
