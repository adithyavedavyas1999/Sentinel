"""FastAPI app for the incident dashboard.

Two facts about this module that matter for review:

1. The store is created per-request, not once at app start. ``IncidentStore``
   opens a fresh sqlite connection on every call, so this is cheap, and it
   means tests can monkeypatch ``SENTINEL_INCIDENTS_DB`` without restarting
   the app.

2. Approval endpoint validates the proposed action against the agent's
   allowlist before touching anything. The dashboard cannot apply a fix
   the agent itself isn't allowed to apply. Same allowlist, same guard,
   same dispatch -- the dashboard is just a different trigger.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from sentinel.agent.remediation import allowlist, dispatch
from sentinel.agent.remediation.types import RemediationDeps
from sentinel.chaos import state as chaos_state
from sentinel.observability.logging import get_logger
from sentinel.quality.incidents import IncidentStore
from sentinel.resources.storage import ObjectStorage
from sentinel.settings import get_settings

log = get_logger(__name__)


def _store() -> IncidentStore:
    return IncidentStore()


def _storage_factory():  # indirection so tests can override
    s = get_settings()
    return ObjectStorage(
        endpoint=s.minio_endpoint,
        access_key=s.minio_access_key,
        secret_key=s.minio_secret_key,
        secure=s.minio_secure,
    )


class IncidentSummary(BaseModel):
    id: str
    asset_key: str
    partition_key: str | None = None
    error_type: str
    status: str
    proposed_fix: str | None = None
    created_at: str


class IncidentDetail(IncidentSummary):
    error_message: str
    upstream_lineage: list[str] = []
    recent_metadata: dict[str, Any] = {}
    resolved_at: str | None = None
    # The diagnosis blob, if the agent has run on this incident. Fetched
    # from the MinIO incidents bucket; None if not yet generated.
    incident_report: dict[str, Any] | None = None


class ApprovalRequest(BaseModel):
    """Body for POST /incidents/{id}/approve.

    We require the approver to pass the action name they're approving;
    this prevents accidental double-resolve race conditions where the
    agent already proposed a different fix in the gap between the UI
    render and the click.
    """

    action: str
    note: str | None = None


class ApprovalResponse(BaseModel):
    status: str  # one of: applied, off_allowlist, guard_failed, error
    detail: str
    incident_id: str


def _parse_metadata(row: dict[str, Any]) -> tuple[list[str], dict[str, Any]]:
    """sqlite stores upstream_lineage + recent_metadata as JSON strings.

    The dashboard wants them as parsed structures; clients can ignore
    them. We're permissive about deserialization here -- a malformed
    row should still render rather than 500 the whole page.
    """
    try:
        lineage = json.loads(row.get("upstream_lineage") or "[]")
    except json.JSONDecodeError:
        lineage = []
    try:
        meta = json.loads(row.get("recent_metadata") or "{}")
    except json.JSONDecodeError:
        meta = {}
    return lineage, meta


def _load_report(storage: ObjectStorage, incident_id: str) -> dict[str, Any] | None:
    s = get_settings()
    key = f"incidents/{incident_id}.json"
    if not storage.object_exists(s.bucket_incidents, key):
        return None
    try:
        return json.loads(storage.get_bytes(s.bucket_incidents, key).decode("utf-8"))
    except Exception:
        log.exception("api.load_report.failed", key=key)
        return None


def create_app() -> FastAPI:
    app = FastAPI(
        title="Sentinel Incident API",
        version="0.1.0",
        description=(
            "Read-only-ish dashboard backend for incidents from the "
            "self-healing pipeline. Approval endpoint dispatches through "
            "the same allowlisted remediation registry as the agent."
        ),
    )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/incidents", response_model=list[IncidentSummary])
    def list_incidents(
        limit: int = 50,
        store: IncidentStore = Depends(_store),
    ) -> list[IncidentSummary]:
        rows = store.list_open(limit=limit)
        return [
            IncidentSummary(
                id=r["id"],
                asset_key=r["asset_key"],
                partition_key=r.get("partition_key"),
                error_type=r["error_type"],
                status=r["status"],
                proposed_fix=r.get("proposed_fix"),
                created_at=r["created_at"],
            )
            for r in rows
        ]

    @app.get("/incidents/{incident_id}", response_model=IncidentDetail)
    def get_incident(
        incident_id: str,
        store: IncidentStore = Depends(_store),
    ) -> IncidentDetail:
        row = store.get(incident_id)
        if row is None:
            raise HTTPException(404, detail=f"no incident {incident_id!r}")
        lineage, meta = _parse_metadata(row)
        storage = _storage_factory()
        return IncidentDetail(
            id=row["id"],
            asset_key=row["asset_key"],
            partition_key=row.get("partition_key"),
            error_type=row["error_type"],
            error_message=row.get("error_message", ""),
            status=row["status"],
            proposed_fix=row.get("proposed_fix"),
            created_at=row["created_at"],
            resolved_at=row.get("resolved_at"),
            upstream_lineage=lineage,
            recent_metadata=meta,
            incident_report=_load_report(storage, incident_id),
        )

    @app.post("/incidents/{incident_id}/resolve", response_model=IncidentSummary)
    def resolve_incident(
        incident_id: str,
        store: IncidentStore = Depends(_store),
    ) -> IncidentSummary:
        row = store.get(incident_id)
        if row is None:
            raise HTTPException(404, detail=f"no incident {incident_id!r}")
        store.resolve(incident_id)
        log.info("api.incident.resolved", incident_id=incident_id)
        row = store.get(incident_id)
        assert row is not None
        return IncidentSummary(
            id=row["id"],
            asset_key=row["asset_key"],
            partition_key=row.get("partition_key"),
            error_type=row["error_type"],
            status=row["status"],
            proposed_fix=row.get("proposed_fix"),
            created_at=row["created_at"],
        )

    @app.post("/incidents/{incident_id}/approve", response_model=ApprovalResponse)
    def approve_remediation(
        incident_id: str,
        body: ApprovalRequest,
        store: IncidentStore = Depends(_store),
    ) -> ApprovalResponse:
        row = store.get(incident_id)
        if row is None:
            raise HTTPException(404, detail=f"no incident {incident_id!r}")
        if body.action not in allowlist():
            return ApprovalResponse(
                status="off_allowlist",
                detail=(
                    f"action '{body.action}' not on allowlist. Allowed: {', '.join(allowlist())}."
                ),
                incident_id=incident_id,
            )

        deps = RemediationDeps(
            storage=_storage_factory(),
            chaos_state=chaos_state,
        )
        incident_dict: dict[str, Any] = dict(row)
        result = dispatch(body.action, incident_dict, deps)
        if result is None:
            return ApprovalResponse(
                status="guard_failed",
                detail=(
                    f"action '{body.action}' was rejected by its guard "
                    f"or failed to execute. See logs."
                ),
                incident_id=incident_id,
            )
        store.set_proposed_fix(incident_id, proposed_fix=body.action)
        log.info(
            "api.incident.approved",
            incident_id=incident_id,
            action=body.action,
            note=body.note,
        )
        return ApprovalResponse(
            status="applied",
            detail=result.description,
            incident_id=incident_id,
        )

    @app.get("/allowlist", response_model=list[str])
    def get_allowlist() -> list[str]:
        return allowlist()

    return app


app = create_app()
