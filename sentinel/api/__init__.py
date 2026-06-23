"""Minimal FastAPI surface for the incident dashboard.

Two read endpoints (``/incidents``, ``/incidents/{id}``), one health
check, one mutating endpoint (``/incidents/{id}/resolve``) to clear an
incident from a human review queue. The remediation-approval endpoint
issues a Dagster run via the same RunRequest tags the agent's
remediation actions use, so dashboard-approved fixes are
indistinguishable from agent-auto fixes in the audit trail.

Surface intentionally small. Two reasons:

1. The reviewers' eye should land on the *agent's* code, not on a sprawl
   of CRUD that's better-built-elsewhere. Anything beyond list/show/approve
   should probably live in Dagster's own UI or in Grafana.
2. Less code means fewer places for the "but the dashboard says X and
   the database says Y" class of bugs.

The app is exposed via ``sentinel.api:app`` so ``uvicorn`` can pick it
up without a separate factory function. ``create_app`` exists for
tests; production lives behind the module-level instance.
"""

from __future__ import annotations

from sentinel.api.main import app, create_app

__all__ = ["app", "create_app"]
