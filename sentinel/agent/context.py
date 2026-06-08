"""Build the context bag the diagnostic agent reads.

Five sources, all optional — if a source is unavailable the bundle just
omits that field and the agent works with what it has.

1. The incident row itself (sqlite). The cheapest, most informative source.
2. dbt lineage. The manifest at ``dbt/target/manifest.json`` knows which
   models depend on which sources; that's most of the "where did the
   bad value come from" question pre-answered.
3. Recent run history for the failing asset's run, via Dagster's instance
   API. We don't go through the GraphQL endpoint because it requires the
   webserver to be up; for the agent we want this to work from the
   daemon's process.
4. Recent log lines from the failed run. Capped — long stack traces from
   dbt or polars are noisy and hurt the agent's signal-to-noise.
5. Top-K similar past incidents from Qdrant. Optional; if Qdrant isn't
   reachable, we skip and log it.

The output is a single :class:`IncidentContext` dataclass meant to be
serialized verbatim into the LangGraph state in week 9. Adding a field
here means adding it everywhere downstream, so be deliberate.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from sentinel.agent.embeddings import (
    IncidentIndex,
    SimilarIncident,
    summarize_for_embedding,
)
from sentinel.observability.logging import get_logger
from sentinel.quality.incidents import IncidentStore

log = get_logger(__name__)

_DEFAULT_MANIFEST_PATH = Path("dbt/target/manifest.json")


@dataclass
class IncidentContext:
    incident: dict[str, Any]
    upstream: list[str] = field(default_factory=list)
    downstream: list[str] = field(default_factory=list)
    dbt_node: dict[str, Any] | None = None
    recent_runs: list[dict[str, Any]] = field(default_factory=list)
    recent_logs: list[str] = field(default_factory=list)
    similar_incidents: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build(
    incident_id: str,
    *,
    incident_store: IncidentStore | None = None,
    dbt_manifest_path: Path | None = None,
    dagster_instance: Any | None = None,
    incident_index: IncidentIndex | None = None,
    log_lines_limit: int = 200,
    similar_top_k: int = 5,
) -> IncidentContext:
    store = incident_store or IncidentStore()
    incident = store.get(incident_id)
    if incident is None:
        raise KeyError(f"incident not found: {incident_id}")

    # JSON columns come back as strings from sqlite; normalize so callers
    # don't trip on type-mixed access.
    for k in ("upstream_lineage", "recent_metadata", "sample_rows"):
        v = incident.get(k)
        if isinstance(v, str):
            try:
                incident[k] = json.loads(v) if v else None
            except json.JSONDecodeError:
                incident[k] = None

    asset_key = incident.get("asset_key", "")

    upstream, downstream, dbt_node = _from_manifest(
        asset_key,
        path=dbt_manifest_path or _DEFAULT_MANIFEST_PATH,
    )

    run_id = (incident.get("recent_metadata") or {}).get("run_id")
    recent_runs = _recent_runs(asset_key, dagster_instance, limit=10)
    recent_logs = _recent_logs(run_id, dagster_instance, limit=log_lines_limit)

    similar = _similar(incident, incident_index, asset_key=asset_key, top_k=similar_top_k)

    return IncidentContext(
        incident=incident,
        upstream=upstream,
        downstream=downstream,
        dbt_node=dbt_node,
        recent_runs=recent_runs,
        recent_logs=recent_logs,
        similar_incidents=[asdict(s) for s in similar],
    )


def _from_manifest(
    asset_key: str,
    *,
    path: Path,
) -> tuple[list[str], list[str], dict[str, Any] | None]:
    if not path.exists():
        return [], [], None

    try:
        manifest = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as e:
        log.warning("context.manifest_unreadable", path=str(path), err=str(e))
        return [], [], None

    nodes: dict[str, Any] = manifest.get("nodes", {})
    parent_map: dict[str, list[str]] = manifest.get("parent_map", {})
    child_map: dict[str, list[str]] = manifest.get("child_map", {})

    # asset_keys from dagster-dbt look like "model_name" or
    # "schema/model_name". We match by name suffix.
    bare = asset_key.split("/")[-1].split(".")[-1]
    node_id = next(
        (
            nid
            for nid, n in nodes.items()
            if n.get("name") == bare and nid.startswith(("model.", "seed.", "snapshot."))
        ),
        None,
    )
    if node_id is None:
        return [], [], None

    return (
        list(parent_map.get(node_id, [])),
        list(child_map.get(node_id, [])),
        nodes.get(node_id),
    )


def _recent_runs(
    asset_key: str,
    instance: Any | None,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    if instance is None:
        return []
    try:
        records = instance.get_run_records(limit=limit)
    except Exception as e:
        log.warning("context.runs_unavailable", err=str(e))
        return []

    out: list[dict[str, Any]] = []
    for rec in records:
        run = getattr(rec, "dagster_run", rec)
        out.append(
            {
                "run_id": getattr(run, "run_id", None),
                "status": str(getattr(run, "status", "")),
                "asset_keys": _asset_keys_for_run(run),
                "tags": dict(getattr(run, "tags", {}) or {}),
                "create_timestamp": getattr(rec, "create_timestamp", None),
            }
        )
    # filter to runs that touched this asset, if asset_key is known
    if asset_key:
        out = [r for r in out if not r["asset_keys"] or asset_key in r["asset_keys"]]
    return out


def _asset_keys_for_run(run: Any) -> list[str]:
    asset_selection = getattr(run, "asset_selection", None) or set()
    keys: list[str] = []
    for k in asset_selection:
        try:
            keys.append(k.to_user_string())
        except AttributeError:
            keys.append(str(k))
    return keys


def _recent_logs(
    run_id: str | None,
    instance: Any | None,
    *,
    limit: int,
) -> list[str]:
    if not run_id or instance is None:
        return []
    try:
        logs = instance.all_logs(run_id, of_type=None)
    except Exception as e:
        log.warning("context.logs_unavailable", err=str(e), run_id=run_id)
        return []

    lines: list[str] = []
    for r in logs:
        # dagster log records expose .user_message or .message. fall back gracefully.
        msg = getattr(r, "user_message", None) or getattr(r, "message", None) or str(r)
        lines.append(msg)
    return lines[-limit:]


def _similar(
    incident: dict[str, Any],
    index: IncidentIndex | None,
    *,
    asset_key: str,
    top_k: int,
) -> list[SimilarIncident]:
    if index is None:
        return []
    try:
        return index.search(
            summarize_for_embedding(incident),
            top_k=top_k,
            asset_key=asset_key or None,
        )
    except Exception as e:
        log.warning("context.qdrant_unavailable", err=str(e))
        return []
