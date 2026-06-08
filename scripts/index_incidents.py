"""One-shot: re-index every captured incident into Qdrant.

Useful when:

- the qdrant volume gets nuked (docker compose down -v)
- we change the embedding model
- we add new fields to the embedding text and want history reflected

Idempotent — upserting by point id is a noop on identical content.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from typing import Any

from sentinel.agent.embeddings import IncidentIndex, summarize_for_embedding
from sentinel.observability.logging import configure_logging, get_logger
from sentinel.quality.incidents import IncidentStore

log = get_logger(__name__)


def _all_incidents(store: IncidentStore) -> list[dict[str, Any]]:
    with sqlite3.connect(store.path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT * FROM incidents ORDER BY created_at ASC")
        return [dict(r) for r in cur.fetchall()]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--qdrant-url", default="http://localhost:6333")
    parser.add_argument("--collection", default="incidents")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="cap rows for a smoke test; default = everything",
    )
    args = parser.parse_args()
    configure_logging(level="INFO", json=False)

    store = IncidentStore()
    rows = _all_incidents(store)
    if args.limit:
        rows = rows[: args.limit]

    if not rows:
        log.info("backfill.empty")
        return 0

    index = IncidentIndex(url=args.qdrant_url, collection=args.collection)
    index.ensure_collection()

    n_indexed = 0
    for r in rows:
        text = summarize_for_embedding(r)
        if not text.strip():
            log.warning("backfill.skipping_empty", incident_id=r.get("id"))
            continue
        index.upsert(
            incident_id=r["id"],
            text=text,
            payload={
                "incident_id": r["id"],
                "asset_key": r.get("asset_key", ""),
                "error_type": r.get("error_type", ""),
                "error_message": r.get("error_message", ""),
                "created_at": r.get("created_at", ""),
                "status": r.get("status", ""),
            },
        )
        n_indexed += 1

    log.info("backfill.done", indexed=n_indexed, total=len(rows))
    return 0


if __name__ == "__main__":
    sys.exit(main())
