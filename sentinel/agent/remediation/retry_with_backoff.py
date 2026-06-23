"""Retry-with-backoff remediation.

Applies to transient ingest failures: TLC 5xx, Open-Meteo 429, and the
matching chaos flags (``tlc_5xx``, ``weather_429``). The action itself
does very little — it clears the chaos flag if present and emits a
``NextRunSpec`` so the sensor knows which partition to re-materialize.
The actual exponential backoff lives inside the ingest functions
(``sentinel.ingest.tlc._get``, ``sentinel.ingest.weather._get_json``),
which are wrapped with tenacity. We're not duplicating backoff here;
we're just clearing the flag and letting tenacity do its thing on the
re-run.

Why this is on the allowlist:

- Idempotent: re-running a bronze partition is cheap and safe.
- Bounded blast radius: never touches silver/gold.
- Rollback is trivial: re-set the flag.
"""

from __future__ import annotations

from typing import Any

from sentinel.observability.logging import get_logger

from .types import NextRunSpec, RemediationDeps, RemediationResult

log = get_logger(__name__)

# Map (asset_key fragment / error fragment) to the flag we should clear.
# Order matters; first match wins. The agent's diagnose step picks the
# action name, but only this table decides which flag to flip. Keeping
# that decision here avoids prompt-injection-shaped bugs.
_FLAG_MAP: tuple[tuple[str, str], ...] = (
    ("tlc_5xx", "tlc_5xx"),
    ("tlc_yellow", "tlc_5xx"),  # error path: real TLC 5xx (not chaos)
    ("weather_429", "weather_429"),
    ("weather_nyc_daily", "weather_429"),
)


def _pick_flag(incident: dict[str, Any]) -> str | None:
    blob = " ".join(
        str(incident.get(k, "")).lower() for k in ("asset_key", "error_message", "error_type")
    )
    for needle, flag in _FLAG_MAP:
        if needle.lower() in blob:
            return flag
    return None


class RetryWithBackoffAction:
    name = "retry-with-backoff"

    def guard(self, incident: dict[str, Any]) -> bool:
        # Must target a bronze asset (we never auto-retry silver/gold) and
        # must have a partition key (we won't blanket-retry an entire asset).
        asset_key = (incident.get("asset_key") or "").lower()
        if not asset_key.startswith("bronze"):
            return False
        if not incident.get("partition_key"):
            return False
        # And must look like a transient upstream failure (not a schema bug
        # the agent miscategorized).
        return _pick_flag(incident) is not None or any(
            sub in (incident.get("error_type") or "")
            for sub in ("HTTPStatusError", "TimeoutException", "ChaosTriggered")
        )

    def execute(self, incident: dict[str, Any], deps: RemediationDeps) -> RemediationResult:
        flag = _pick_flag(incident)
        cleared_flag: str | None = None
        if flag and deps.chaos_state is not None:
            if bool(deps.chaos_state.clear(flag)):
                cleared_flag = flag
            log.info("remediation.retry.flag_cleared", flag=flag, ok=cleared_flag is not None)

        # Build run tags. Only stamp the cleared-flag tag when we actually
        # removed an active flag -- picking the name from the heuristic map
        # without clearing anything would mislead the audit log on the
        # downstream run.
        run_tags: dict[str, str] = {
            "sentinel.remediation": self.name,
            "sentinel.incident_id": str(incident.get("id", "")),
        }
        if cleared_flag is not None:
            run_tags["sentinel.cleared_flag"] = cleared_flag

        next_run = NextRunSpec(
            asset_key=incident["asset_key"],
            partition_key=incident.get("partition_key"),
            run_tags=run_tags,
        )
        return RemediationResult(
            action=self.name,
            description=(
                f"cleared chaos flag '{cleared_flag}' and queued re-materialization"
                if cleared_flag
                else "queued re-materialization (no flag to clear)"
            ),
            success=True,
            next_run=next_run,
            rollback_data={"cleared_flag": cleared_flag},
        )

    def rollback(self, result: RemediationResult, deps: RemediationDeps) -> None:
        flag = result.rollback_data.get("cleared_flag")
        if flag and deps.chaos_state is not None:
            log.info("remediation.retry.rollback.flag_reset", flag=flag)
            deps.chaos_state.set_active(flag)
