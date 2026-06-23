"""Shared types for remediation actions.

Lives in its own module so the action implementations and the registry
don't form an import cycle, and so the protocol in
``sentinel.agent.remediation`` is the only public surface a caller
needs to consume.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class NextRunSpec:
    """A small, JSON-friendly description of a follow-up materialization.

    We deliberately avoid building a Dagster ``RunRequest`` here — the
    remediator sensor owns that. Keeping the action layer pure lets us
    fixture-test it without spinning up a Dagster instance.
    """

    asset_key: str
    partition_key: str | None = None
    run_tags: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class RemediationResult:
    """What the action did and what should happen next.

    ``rollback_data`` is whatever the action needs to undo itself in
    case the follow-up run fails. For ``retry-with-backoff`` that's
    "which flag we cleared". For ``coerce-to-string`` it's the
    quarantine key we wrote so we can delete it.
    """

    action: str
    description: str
    success: bool
    next_run: NextRunSpec | None = None
    rollback_data: dict[str, Any] = field(default_factory=dict)


@dataclass
class RemediationDeps:
    """Read/write deps the action needs.

    Frozen-ish, but not actually frozen because we accept callers
    threading in test fakes for ``storage`` and ``chaos_state``.
    """

    storage: Any | None = None  # ObjectStorage; typed loose to keep tests lighter
    chaos_state: Any | None = None  # module ref so tests can stub it out
    bucket_bronze: str = "sentinel-raw"
    bucket_incidents: str = "sentinel-incidents"
