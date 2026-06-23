"""Allowlisted remediation actions for the agent.

Three actions today, mirroring the ADR-004 allowlist:

- ``retry-with-backoff`` — transient ingest failure (5xx, 429, timeout):
  clear the chaos flag (if any) and re-emit the bronze partition.
- ``partition-window-slip`` — upstream hasn't published the partition:
  re-emit using ``D-1`` instead. The agent annotates the run with a
  ``partition_slipped_from`` tag so a human can audit the choice.
- ``coerce-to-string`` — bronze column type widened unexpectedly: rewrite
  the affected column as utf8 and route the rows to a quarantine bucket.
  Silver/gold never see them.

Each action implements the same surface (``guard``/``execute``/
``rollback``) and returns a typed ``RemediationResult``. Anything outside
this module — graph nodes, sensors, future remediator job — sees actions
through the small dispatch helpers at the bottom of this file. That
keeps the "don't ever drift off the allowlist" invariant in exactly one
place.

What lives here vs. what the *sensor* does:

- Actions are pure functions of (incident, deps). They never touch
  Dagster's run loop directly.
- The remediator sensor (week 10) reads the action's ``next_run``
  description and issues the RunRequest. That gives us a deterministic
  fixture-replay story for tests.
"""

from __future__ import annotations

from typing import Any, Protocol

from sentinel.agent.remediation.coerce_to_string import CoerceToStringAction
from sentinel.agent.remediation.partition_window_slip import PartitionWindowSlipAction
from sentinel.agent.remediation.retry_with_backoff import RetryWithBackoffAction
from sentinel.agent.remediation.types import (
    NextRunSpec,
    RemediationDeps,
    RemediationResult,
)
from sentinel.observability.logging import get_logger

log = get_logger(__name__)


class RemediationAction(Protocol):
    """Uniform protocol every action must satisfy.

    Implementations live in sibling modules. We deliberately don't make
    this an ABC: structural typing keeps the test stubs lighter.
    """

    name: str

    def guard(self, incident: dict[str, Any]) -> bool:
        """True iff the action is safe and applicable to this incident."""

    def execute(self, incident: dict[str, Any], deps: RemediationDeps) -> RemediationResult:
        """Perform any pre-run side effects (clear flag, write quarantine, ...).

        Returns a result describing what (if anything) needs to happen
        next (e.g. a RunRequest for the orchestrator to issue).
        """

    def rollback(self, result: RemediationResult, deps: RemediationDeps) -> None:
        """Undo whatever ``execute`` did. Best-effort; some actions can't undo."""


_REGISTRY: dict[str, RemediationAction] = {
    "retry-with-backoff": RetryWithBackoffAction(),
    "partition-window-slip": PartitionWindowSlipAction(),
    "coerce-to-string": CoerceToStringAction(),
}


def get_action(name: str) -> RemediationAction | None:
    """Look up an action by its allowlist name. Returns None if off-list."""
    return _REGISTRY.get(name)


def allowlist() -> list[str]:
    """Public list of allowlisted action names. Stable -- callers may sort/format."""
    return sorted(_REGISTRY.keys())


def dispatch(
    proposed_fix: str,
    incident: dict[str, Any],
    deps: RemediationDeps,
) -> RemediationResult | None:
    """End-to-end: find the action, guard, execute. Returns None if off-list or guard fails.

    Failure here is non-fatal — the caller (sensor) just doesn't remediate
    and keeps the incident open for human review. We never silently take
    a non-allowlisted action.
    """
    action = get_action(proposed_fix)
    if action is None:
        log.warning("remediation.dispatch.off_allowlist", proposed_fix=proposed_fix)
        return None
    if not action.guard(incident):
        log.warning(
            "remediation.dispatch.guard_failed",
            action=action.name,
            incident_id=incident.get("id"),
        )
        return None
    try:
        return action.execute(incident, deps)
    except Exception:  # rollback path
        log.exception("remediation.dispatch.execute_failed", action=action.name)
        return None


__all__ = [
    "NextRunSpec",
    "RemediationAction",
    "RemediationDeps",
    "RemediationResult",
    "allowlist",
    "dispatch",
    "get_action",
]
