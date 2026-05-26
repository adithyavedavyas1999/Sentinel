"""Sensor that detects when TLC publishes a new monthly parquet upstream.

TLC publishes mid-month for the previous month. We HEAD-check the URL for the
next month after the latest materialized partition; if it 200s, we kick off a
run for that partition.

Lightweight on purpose. Real freshness SLAs would compare published time to
materialization time and alert; this is closer to "automatic catch-up".
"""
from __future__ import annotations

from datetime import date, timedelta

import httpx
from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from sentinel.assets.bronze.tlc import bronze_tlc_yellow, monthly_partitions
from sentinel.ingest import tlc


def _next_month(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


@sensor(
    asset_selection=[bronze_tlc_yellow],
    minimum_interval_seconds=60 * 60,  # hourly is plenty for monthly drops
    default_status=DefaultSensorStatus.STOPPED,  # opt-in; running in dev is noisy
)
def tlc_freshness_sensor(context: SensorEvaluationContext):
    materialized = monthly_partitions.get_partition_keys(
        current_time=context.last_tick_completion_time
    )
    if not materialized:
        return SkipReason("no partitions yet")

    # Partitions are sorted ascending by Dagster; take the latest, propose the
    # next one if it exists upstream.
    latest = date.fromisoformat(materialized[-1])
    candidate = _next_month(latest)
    if candidate > date.today() - timedelta(days=1):
        return SkipReason(f"next partition {candidate} not yet expected upstream")

    url = tlc.yellow_url(candidate.year, candidate.month)
    try:
        # HEAD only — don't pull tens of megabytes just to check existence.
        resp = httpx.head(url, timeout=10.0, follow_redirects=True)
    except httpx.HTTPError as exc:
        return SkipReason(f"upstream HEAD failed: {exc}")

    if resp.status_code != 200:
        return SkipReason(f"upstream not yet published ({resp.status_code})")

    return RunRequest(
        run_key=f"tlc-{candidate.isoformat()}",
        partition_key=candidate.isoformat(),
    )
