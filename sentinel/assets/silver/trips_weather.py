"""DEPRECATED: replaced by dbt/models/marts/silver_trips_weather.sql in week 4.

Kept around briefly while the backfill catches up. Delete once dbt silver has
run cleanly for two consecutive partitions. See ADR-001 / roadmap week 4.
"""
# no `from __future__ import annotations` here: dagster's asset decorator
# validates context type hints against the actual class, not strings.

from io import BytesIO

import polars as pl
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    asset,
    asset_check,
)

from sentinel.assets.bronze.tlc import monthly_partitions
from sentinel.resources.storage import ObjectStorage
from sentinel.settings import get_settings


def _bronze_tlc_key(year: int, month: int) -> str:
    return (
        f"bronze/tlc/yellow/year={year:04d}/month={month:02d}/"
        f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    )


def _bronze_weather_key(year: int, month: int) -> str:
    return (
        f"bronze/weather/nyc/year={year:04d}/month={month:02d}/"
        f"weather_nyc_{year:04d}-{month:02d}.parquet"
    )


def _silver_key(year: int, month: int) -> str:
    return (
        f"silver/trips_weather/year={year:04d}/month={month:02d}/"
        f"trips_weather_{year:04d}-{month:02d}.parquet"
    )


# TODO(week-4): this is the throwaway python silver. dbt will own this join.
@asset(
    key=AssetKey(["silver", "trips_weather"]),
    partitions_def=monthly_partitions,
    group_name="silver",
    compute_kind="python",
    ins={
        "_tlc": AssetIn(AssetKey(["bronze", "tlc_yellow"])),
        "_weather": AssetIn(AssetKey(["bronze", "weather_nyc_daily"])),
    },
    description="Interim python silver. Joins TLC trips to NYC daily weather on pickup date.",
)
def silver_trips_weather(
    context: AssetExecutionContext,
    storage: ObjectStorage,
    _tlc,
    _weather,
) -> MaterializeResult:
    settings = get_settings()
    bucket = settings.bucket_bronze

    p = context.partition_key
    year, month = int(p[:4]), int(p[5:7])

    # Read bronze parquets straight from MinIO. We *could* push this into
    # DuckDB but we're throwing this code away anyway.
    tlc_bytes = _read_object(storage, bucket, _bronze_tlc_key(year, month))
    wx_bytes = _read_object(storage, bucket, _bronze_weather_key(year, month))

    trips = pl.read_parquet(BytesIO(tlc_bytes))
    weather = pl.read_parquet(BytesIO(wx_bytes))

    # TLC schema has changed historically. Be defensive about column names.
    pickup_col = _first_present(trips.columns, ["tpep_pickup_datetime", "pickup_datetime"])
    if pickup_col is None:
        raise ValueError(f"no pickup datetime column found; got {trips.columns!r}")

    trips_clean = (
        trips.lazy()
        .with_columns(pl.col(pickup_col).cast(pl.Datetime, strict=False).alias("pickup_ts"))
        .filter(pl.col("pickup_ts").is_not_null())
        .with_columns(pl.col("pickup_ts").dt.date().alias("pickup_date"))
        .collect()
    )

    joined = trips_clean.join(weather, left_on="pickup_date", right_on="date", how="left")

    out = BytesIO()
    joined.write_parquet(out, compression="zstd")
    payload = out.getvalue()

    storage.put_bytes(
        bucket,
        _silver_key(year, month),
        payload,
        content_type="application/x-parquet",
    )

    return MaterializeResult(
        metadata={
            "key": _silver_key(year, month),
            "rows": MetadataValue.int(joined.height),
            "bytes": MetadataValue.int(len(payload)),
            "pickup_col": pickup_col,
        },
    )


def _read_object(storage: ObjectStorage, bucket: str, key: str) -> bytes:
    client = storage._client()
    resp = client.get_object(bucket, key)
    try:
        return resp.read()
    finally:
        resp.close()
        resp.release_conn()


def _first_present(cols: list[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in cols:
            return c
    return None


@asset_check(asset=silver_trips_weather, name="rowcount_positive")
def check_rowcount_positive(
    context: AssetExecutionContext,
    storage: ObjectStorage,
) -> AssetCheckResult:
    settings = get_settings()
    p = context.partition_key
    year, month = int(p[:4]), int(p[5:7])
    data = _read_object(storage, settings.bucket_bronze, _silver_key(year, month))
    df = pl.read_parquet(BytesIO(data))
    rows = df.height
    return AssetCheckResult(
        passed=rows > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": MetadataValue.int(rows)},
    )


@asset_check(asset=silver_trips_weather, name="pickup_ts_not_null")
def check_pickup_ts_not_null(
    context: AssetExecutionContext,
    storage: ObjectStorage,
) -> AssetCheckResult:
    settings = get_settings()
    p = context.partition_key
    year, month = int(p[:4]), int(p[5:7])
    data = _read_object(storage, settings.bucket_bronze, _silver_key(year, month))
    df = pl.read_parquet(BytesIO(data))
    nulls = df.filter(pl.col("pickup_ts").is_null()).height
    return AssetCheckResult(
        passed=nulls == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={"null_pickup_ts": MetadataValue.int(nulls)},
    )
