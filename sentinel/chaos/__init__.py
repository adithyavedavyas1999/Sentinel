"""Chaos injection harness.

Each scenario is a function that mutates state in a controlled, reversible way.
The Phase 2 agent evaluation suite runs these and grades the agent's response.

Right now this is a stub registry — the actual mutations land in Phase 2 when
we have the agent to evaluate against. Detection is what matters today.
"""
from __future__ import annotations

from collections.abc import Callable
from io import BytesIO

import polars as pl

from sentinel.observability.logging import get_logger
from sentinel.resources import ObjectStorage
from sentinel.settings import get_settings

log = get_logger(__name__)


def _storage() -> ObjectStorage:
    s = get_settings()
    return ObjectStorage(
        endpoint=s.minio_endpoint,
        access_key=s.minio_access_key,
        secret_key=s.minio_secret_key,
        secure=s.minio_secure,
    )


def tlc_schema_drift(dry_run: bool) -> int:
    """Rewrite the most recent TLC bronze parquet with a renamed column.

    Real TLC has done exactly this — e.g. when they renamed `VendorID` casing.
    Should trip stg_tlc_yellow's schema fingerprint check.
    """
    s = get_settings()
    storage = _storage()
    keys = list(storage.list_keys(s.bucket_bronze, "bronze/tlc/yellow/"))
    if not keys:
        log.warning("chaos.tlc_schema_drift.no_data")
        return 1
    target = sorted(keys)[-1]
    log.info("chaos.tlc_schema_drift", target=target, dry_run=dry_run)
    if dry_run:
        return 0

    # download, mutate, re-upload
    client = storage._client()
    resp = client.get_object(s.bucket_bronze, target)
    try:
        data = resp.read()
    finally:
        resp.close()
        resp.release_conn()

    df = pl.read_parquet(BytesIO(data))
    if "VendorID" not in df.columns:
        log.warning("chaos.tlc_schema_drift.no_vendor_id_column")
        return 1
    df = df.rename({"VendorID": "vendor_id_renamed"})
    buf = BytesIO()
    df.write_parquet(buf, compression="zstd")
    storage.put_bytes(s.bucket_bronze, target, buf.getvalue(), content_type="application/x-parquet")
    return 0


def null_spike(dry_run: bool) -> int:
    """Stuff nulls into pickup_ts for a small slice of the latest bronze.

    Should trip the dbt not_null tests on stg_tlc_yellow.pickup_ts.
    """
    log.info("chaos.null_spike.todo", dry_run=dry_run)
    # TODO(phase-2): implement; needs a roundtrip read/write of a slice.
    return 0


def _todo(name: str) -> Callable[[bool], int]:
    def _impl(dry_run: bool) -> int:
        log.info(f"chaos.{name}.todo", dry_run=dry_run)
        return 0

    return _impl


_REGISTRY: dict[str, Callable[[bool], int]] = {
    "tlc_5xx": _todo("tlc_5xx"),
    "tlc_schema_drift": tlc_schema_drift,
    "weather_429": _todo("weather_429"),
    "duckdb_lock": _todo("duckdb_lock"),
    "dbt_sql_error": _todo("dbt_sql_error"),
    "null_spike": null_spike,
    "volume_drop": _todo("volume_drop"),
    "late_partition": _todo("late_partition"),
}


def run(scenario: str, *, dry_run: bool = False) -> int:
    fn = _REGISTRY.get(scenario)
    if fn is None:
        log.error("chaos.unknown_scenario", scenario=scenario)
        return 2
    return fn(dry_run)


if __name__ == "__main__":
    import sys

    raise SystemExit(run(sys.argv[1], dry_run="--dry-run" in sys.argv))
