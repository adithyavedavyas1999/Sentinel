"""Coerce-to-string + quarantine remediation.

Used when a column in bronze widens unexpectedly (TLC has shipped both
``VendorID`` -> ``vendor_id`` casing renames and silent type widens
from int -> string historically). Instead of failing dbt downstream,
we:

1. Read the broken bronze parquet.
2. Cast every column to utf8 (Polars' lossless string coercion).
3. Write the coerced rows to a separate ``_quarantine/`` prefix in
   bronze. Silver/gold never see them.
4. Emit a ``NextRunSpec`` re-materializing the *original* bronze
   partition. That re-run will replace the bad parquet with a fresh
   one from upstream, which will either succeed (transient) or fail
   again (and the quarantine row stays for post-mortem).

Why this is on the allowlist:

- The original bronze parquet is overwritten by the follow-up
  ingest, not by us. We only ever *add* a quarantine artifact.
- Silver/gold are unaffected because they read from bronze, and
  bronze (post-re-ingest) is back to the upstream schema.
- Rollback deletes the quarantine row — safe.

Failure-mode worth documenting: if the upstream is permanently in the
new schema, the re-run will look "successful" but silver will keep
breaking. That's the agent's eval-suite responsibility to catch (it
reads ``recent_metadata`` and notices the re-run + silver failure
pair). For now we log the action and move on.
"""

from __future__ import annotations

from io import BytesIO
from typing import Any

import polars as pl

from sentinel.observability.logging import get_logger

from .types import NextRunSpec, RemediationDeps, RemediationResult

log = get_logger(__name__)


def _quarantine_key(original_key: str) -> str:
    # Mirror the original prefix under bronze/_quarantine/ so it's easy
    # to find by hand. ``bronze/tlc/yellow/year=2024/.../x.parquet`` ->
    # ``bronze/_quarantine/tlc/yellow/year=2024/.../x.parquet``.
    if original_key.startswith("bronze/"):
        return "bronze/_quarantine/" + original_key[len("bronze/") :]
    return f"bronze/_quarantine/{original_key}"


def _find_latest_bronze_key(deps: RemediationDeps, asset_key: str) -> str | None:
    asset_key_lower = asset_key.lower()
    if "tlc" in asset_key_lower:
        prefix = "bronze/tlc/yellow/"
    elif "weather" in asset_key_lower:
        prefix = "bronze/weather/nyc/"
    else:
        return None
    if deps.storage is None:
        return None
    keys = list(deps.storage.list_keys(deps.bucket_bronze, prefix))
    return sorted(keys)[-1] if keys else None


class CoerceToStringAction:
    name = "coerce-to-string"

    def guard(self, incident: dict[str, Any]) -> bool:
        asset_key = (incident.get("asset_key") or "").lower()
        if not asset_key.startswith("bronze"):
            return False
        err = (incident.get("error_message") or "").lower()
        err_type = incident.get("error_type") or ""
        # Schema-shaped failures only. We don't want this action firing on
        # a 5xx (retry-with-backoff is the right answer there).
        looks_schema = (
            any(
                sub in err
                for sub in (
                    "schema",
                    "vendor_id",
                    "temperature_2m",
                    "column",
                    "type",
                    "did not match",
                )
            )
            or "ValidationError" in err_type
            or "SchemaError" in err_type
        )
        return looks_schema

    def execute(self, incident: dict[str, Any], deps: RemediationDeps) -> RemediationResult:
        if deps.storage is None:
            log.warning("remediation.coerce.no_storage")
            return RemediationResult(
                action=self.name, description="no storage available", success=False
            )

        original_key = _find_latest_bronze_key(deps, incident.get("asset_key", ""))
        if original_key is None:
            return RemediationResult(
                action=self.name,
                description="could not locate bronze parquet for asset",
                success=False,
            )

        data = deps.storage.get_bytes(deps.bucket_bronze, original_key)
        df = pl.read_parquet(BytesIO(data))
        # All columns -> utf8. Polars handles ints, floats, datetimes, nulls
        # without raising. Datetimes serialize to ISO-8601 strings which
        # is good enough for a quarantine table.
        coerced = df.with_columns([pl.col(c).cast(pl.Utf8, strict=False) for c in df.columns])

        out = BytesIO()
        coerced.write_parquet(out, compression="zstd")
        payload = out.getvalue()

        qkey = _quarantine_key(original_key)
        deps.storage.put_bytes(
            deps.bucket_bronze,
            qkey,
            payload,
            content_type="application/x-parquet",
        )
        log.info(
            "remediation.coerce.quarantined",
            from_key=original_key,
            to_key=qkey,
            rows=coerced.height,
        )

        next_run = NextRunSpec(
            asset_key=incident["asset_key"],
            partition_key=incident.get("partition_key"),
            run_tags={
                "sentinel.remediation": self.name,
                "sentinel.incident_id": str(incident.get("id", "")),
                "sentinel.quarantine_key": qkey,
            },
        )
        return RemediationResult(
            action=self.name,
            description=f"wrote {coerced.height} coerced rows to {qkey}; queued re-ingest",
            success=True,
            next_run=next_run,
            rollback_data={"quarantine_key": qkey, "bucket": deps.bucket_bronze},
        )

    def rollback(self, result: RemediationResult, deps: RemediationDeps) -> None:
        qkey = result.rollback_data.get("quarantine_key")
        bucket = result.rollback_data.get("bucket")
        if not qkey or not bucket or deps.storage is None:
            return
        try:
            deps.storage.delete_object(bucket, qkey)
            log.info("remediation.coerce.rollback.deleted", key=qkey)
        except Exception:
            log.exception("remediation.coerce.rollback.failed", key=qkey)
