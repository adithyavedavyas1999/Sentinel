import hashlib
from io import BytesIO

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    MonthlyPartitionsDefinition,
    asset,
)

from sentinel.chaos.state import ChaosTriggered
from sentinel.chaos.state import is_active as chaos_active
from sentinel.ingest import tlc
from sentinel.observability.metrics import (
    asset_materializations_total,
    rows_landed_total,
    time_ingest,
)
from sentinel.resources.storage import ObjectStorage
from sentinel.settings import get_settings

# TLC publishes one parquet per (taxi_type, year, month). Start from 2024-01;
# bump start_date if we ever need earlier history.
monthly_partitions = MonthlyPartitionsDefinition(
    start_date="2024-01-01",
    end_offset=-1,
    timezone="America/New_York",
)


def _bronze_key(year: int, month: int) -> str:
    return (
        f"bronze/tlc/yellow/year={year:04d}/month={month:02d}/"
        f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    )


@asset(
    key=AssetKey(["bronze", "tlc_yellow"]),
    partitions_def=monthly_partitions,
    group_name="bronze",
    compute_kind="python",
    description="Yellow taxi trip-data parquet, landed as-is from TLC CloudFront.",
)
def bronze_tlc_yellow(
    context: AssetExecutionContext,
    storage: ObjectStorage,
) -> MaterializeResult:
    settings = get_settings()
    bucket = settings.bucket_bronze

    p = context.partition_key  # YYYY-MM-DD (first of month)
    year, month, _ = (int(x) for x in p.split("-"))
    key = _bronze_key(year, month)

    if chaos_active("tlc_5xx"):
        # phase-2 hook: agent should detect and (per allowlist) retry-with-backoff
        # after the operator clears the flag. for now, just raise.
        raise ChaosTriggered("chaos:tlc_5xx — simulated upstream 5xx")

    if chaos_active("late_partition"):
        # Simulates the partition existing in the schedule before TLC
        # actually publishes it. Agent's expected remediation (week 10) is
        # partition-window-slip — re-run for the previous month.
        raise ChaosTriggered(
            f"chaos:late_partition — partition {p} not yet published upstream (404)"
        )

    if storage.object_exists(bucket, key):
        context.log.info(f"already landed: s3://{bucket}/{key}")
        asset_materializations_total.labels(asset="bronze.tlc_yellow", status="skipped").inc()
        return MaterializeResult(
            metadata={
                "bucket": bucket,
                "key": key,
                "skipped": MetadataValue.bool(True),
            },
        )

    with time_ingest(source="tlc"):
        data = tlc.fetch_yellow_tripdata(year, month)
    bytes_written = storage.put_bytes(bucket, key, data, content_type="application/x-parquet")

    rows, schema_fp, min_ts, max_ts = _profile_tlc_parquet(data)
    asset_materializations_total.labels(asset="bronze.tlc_yellow", status="ok").inc()
    rows_landed_total.labels(asset="bronze.tlc_yellow").inc(rows)

    return MaterializeResult(
        metadata={
            "bucket": bucket,
            "key": key,
            "url": tlc.yellow_url(year, month),
            "bytes": MetadataValue.int(bytes_written),
            "rows": MetadataValue.int(rows),
            "schema_fingerprint": schema_fp,
            "pickup_min": MetadataValue.text(min_ts),
            "pickup_max": MetadataValue.text(max_ts),
            "skipped": MetadataValue.bool(False),
        },
    )


def _profile_tlc_parquet(data: bytes) -> tuple[int, str, str, str]:
    """Profile the just-fetched parquet for metadata + schema fingerprint.

    The fingerprint is the first 16 hex chars of SHA256 over `col:dtype`
    tuples. Good enough to detect drift; not collision-resistant in a
    cryptographic sense, but TLC schemas don't churn that hard.

    Best-effort: returns empty values on parse failure rather than killing
    the materialization. The bytes are already landed at this point.
    """
    try:
        df = pl.read_parquet(BytesIO(data))
    except Exception:  # -- polars wraps a wide variety of errors
        return 0, "", "", ""
    rows = df.height
    schema_str = ",".join(f"{n}:{t}" for n, t in zip(df.columns, df.dtypes, strict=True))
    schema_fp = hashlib.sha256(schema_str.encode()).hexdigest()[:16]

    pickup_col = next(
        (c for c in ("tpep_pickup_datetime", "pickup_datetime") if c in df.columns),
        None,
    )
    if pickup_col is None:
        return rows, schema_fp, "", ""
    try:
        series = df[pickup_col].cast(pl.Datetime, strict=False).drop_nulls()
    except Exception:
        return rows, schema_fp, "", ""
    if series.is_empty():
        return rows, schema_fp, "", ""
    return rows, schema_fp, str(series.min()), str(series.max())
