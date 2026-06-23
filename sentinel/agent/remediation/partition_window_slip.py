"""Partition-window slip remediation.

Used when an upstream source hasn't published the requested partition
yet — most commonly TLC, which can be late by a few weeks. Instead of
failing, slip to the previous partition (D-1) and tag the run so a
human can audit.

The bronze partitions are monthly (``YYYY-MM-01``), so "D-1" here means
"previous month". Year rollover is handled below.

Why allowlist-safe:

- Pure bookkeeping; no data is mutated, we just re-target the next run
  at a different partition key.
- Tagged on the run; auditable.
- Rollback is a no-op (nothing was changed).

Caveats deliberately accepted:

- We don't validate that the previous partition actually exists.
  Validating costs an extra round-trip to the upstream API and rarely
  pays off — if D-1 is missing too we'll just re-incident.
- We slip at most one month. Multi-month slip would require an
  approval gate; that's out of scope per ADR-004.
"""

from __future__ import annotations

from typing import Any

from sentinel.observability.logging import get_logger

from .types import NextRunSpec, RemediationDeps, RemediationResult

log = get_logger(__name__)


def _slip_one_month(partition_key: str) -> str:
    """``YYYY-MM-DD`` -> the first of the previous month, ``YYYY-MM-01``.

    We always normalize day-of-month to 01 because bronze partitions are
    monthly and the slipped key has to land on a real bronze partition.
    Slipping ``2024-01-15`` returns ``2023-12-01``, not ``2023-12-15``.
    """
    try:
        year, month = int(partition_key[:4]), int(partition_key[5:7])
    except (ValueError, TypeError):
        raise ValueError(f"unparseable partition_key: {partition_key!r}") from None
    if month == 1:
        year, month = year - 1, 12
    else:
        month -= 1
    return f"{year:04d}-{month:02d}-01"


class PartitionWindowSlipAction:
    name = "partition-window-slip"

    def guard(self, incident: dict[str, Any]) -> bool:
        # Bronze-only, partition required, and the error has to look like a
        # "partition not yet published" condition (404 or late_partition flag).
        asset_key = (incident.get("asset_key") or "").lower()
        if not asset_key.startswith("bronze"):
            return False
        if not incident.get("partition_key"):
            return False
        err = (incident.get("error_message") or "").lower()
        err_type = incident.get("error_type") or ""
        return (
            "late_partition" in err
            or "not yet published" in err
            or "404" in err
            or "NotFound" in err_type
        )

    def execute(self, incident: dict[str, Any], deps: RemediationDeps) -> RemediationResult:
        original = incident["partition_key"]
        slipped = _slip_one_month(original)

        # Clear the late_partition chaos flag (if any). In real
        # operation no flag exists; the chaos-state clear is a no-op
        # in that case so this is always safe to call.
        if deps.chaos_state is not None:
            deps.chaos_state.clear("late_partition")

        next_run = NextRunSpec(
            asset_key=incident["asset_key"],
            partition_key=slipped,
            run_tags={
                "sentinel.remediation": self.name,
                "sentinel.incident_id": str(incident.get("id", "")),
                "sentinel.partition_slipped_from": original,
            },
        )
        log.info(
            "remediation.partition_slip",
            asset_key=incident["asset_key"],
            from_partition=original,
            to_partition=slipped,
        )
        return RemediationResult(
            action=self.name,
            description=f"slipped partition {original} -> {slipped}",
            success=True,
            next_run=next_run,
            rollback_data={"original_partition": original, "slipped_partition": slipped},
        )

    def rollback(self, result: RemediationResult, deps: RemediationDeps) -> None:
        # No data was changed; nothing to roll back. The follow-up run
        # request, if it failed, will already have logged its own incident.
        log.info(
            "remediation.partition_slip.rollback.noop",
            original_partition=result.rollback_data.get("original_partition"),
        )
