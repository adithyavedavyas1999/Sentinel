"""Incident capture for the Phase 2 self-healing agent to consume.

Schema is deliberately wide and JSON-ish. The agent reads these; the API and
streamlit page (week 11) read these. Anything that might inform diagnosis
goes in.

Storage: SQLite for now. The roadmap originally said Postgres but
Dagster owns the existing postgres DB and I don't want to couple our schema
to dagster's migrations. Move to Postgres when we have real write volume or
need cross-process locking. Until then, file-based is cleaner.
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_DEFAULT_DB = "./data/warehouse/incidents.sqlite"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS incidents (
    id              TEXT PRIMARY KEY,
    created_at      TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'open',
    asset_key       TEXT NOT NULL,
    partition_key   TEXT,
    error_type      TEXT,
    error_message   TEXT,
    stack_trace     TEXT,
    upstream_lineage TEXT,   -- json array
    recent_metadata TEXT,    -- json blob
    sample_rows     TEXT,    -- json array (capped)
    proposed_fix    TEXT,
    resolved_at     TEXT
);

CREATE INDEX IF NOT EXISTS idx_incidents_status_created
    ON incidents(status, created_at);

CREATE INDEX IF NOT EXISTS idx_incidents_asset_partition
    ON incidents(asset_key, partition_key);
"""


@dataclass
class Incident:
    asset_key: str
    error_type: str
    error_message: str
    partition_key: str | None = None
    stack_trace: str | None = None
    upstream_lineage: list[str] = field(default_factory=list)
    recent_metadata: dict[str, Any] = field(default_factory=dict)
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    proposed_fix: str | None = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    status: str = "open"
    resolved_at: str | None = None

    def to_row(self) -> dict[str, Any]:
        d = asdict(self)
        d["upstream_lineage"] = json.dumps(self.upstream_lineage)
        d["recent_metadata"] = json.dumps(self.recent_metadata)
        d["sample_rows"] = json.dumps(self.sample_rows)
        return d


class IncidentStore:
    def __init__(self, path: str | None = None) -> None:
        self.path = path or os.environ.get("SENTINEL_INCIDENTS_DB", _DEFAULT_DB)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as c:
            c.executescript(_SCHEMA)

    @contextmanager
    def _conn(self):
        # check_same_thread=False so the dagster daemon (which is threaded)
        # can write from sensors without grief.
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def insert(self, incident: Incident) -> str:
        row = incident.to_row()
        cols = ",".join(row.keys())
        placeholders = ",".join("?" * len(row))
        with self._conn() as c:
            c.execute(
                f"INSERT INTO incidents ({cols}) VALUES ({placeholders})",
                tuple(row.values()),
            )
        return incident.id

    def list_open(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._conn() as c:
            cur = c.execute(
                "SELECT * FROM incidents WHERE status='open' ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]

    def get(self, incident_id: str) -> dict[str, Any] | None:
        with self._conn() as c:
            cur = c.execute("SELECT * FROM incidents WHERE id=?", (incident_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def resolve(self, incident_id: str) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE incidents SET status='resolved', resolved_at=? WHERE id=?",
                (datetime.now(UTC).isoformat(), incident_id),
            )

    def set_proposed_fix(self, incident_id: str, *, proposed_fix: str) -> None:
        """Stamp the agent's proposed fix back on the incident row.

        Idempotent -- safe to re-call with the same value (e.g. the agent
        sensor re-ticks on a row that was already diagnosed but somehow
        didn't get its MinIO blob). The unique status/resolved fields are
        left alone here; resolution is a separate human/auto action.
        """
        with self._conn() as c:
            c.execute(
                "UPDATE incidents SET proposed_fix=? WHERE id=?",
                (proposed_fix, incident_id),
            )
