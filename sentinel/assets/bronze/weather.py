from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    asset,
)

from sentinel.assets.bronze.tlc import monthly_partitions
from sentinel.ingest import weather
from sentinel.observability.metrics import (
    asset_materializations_total,
    rows_landed_total,
    time_ingest,
)
from sentinel.resources.storage import ObjectStorage
from sentinel.settings import get_settings


def _bronze_key(year: int, month: int) -> str:
    return (
        f"bronze/weather/nyc/year={year:04d}/month={month:02d}/"
        f"weather_nyc_{year:04d}-{month:02d}.parquet"
    )


@asset(
    key=AssetKey(["bronze", "weather_nyc_daily"]),
    partitions_def=monthly_partitions,
    group_name="bronze",
    compute_kind="python",
    description="Open-Meteo daily weather archive for NYC, one month per partition.",
)
def bronze_weather_nyc_daily(
    context: AssetExecutionContext,
    storage: ObjectStorage,
) -> MaterializeResult:
    settings = get_settings()
    bucket = settings.bucket_bronze

    p = context.partition_key
    year, month, _ = (int(x) for x in p.split("-"))
    key = _bronze_key(year, month)

    if storage.object_exists(bucket, key):
        context.log.info(f"already landed: s3://{bucket}/{key}")
        asset_materializations_total.labels(
            asset="bronze.weather_nyc_daily", status="skipped"
        ).inc()
        return MaterializeResult(
            metadata={
                "bucket": bucket,
                "key": key,
                "skipped": MetadataValue.bool(True),
            },
        )

    with time_ingest(source="open-meteo"):
        payload = weather.fetch_daily_archive(
            year,
            month,
            lat=settings.weather_lat,
            lon=settings.weather_lon,
            tz=settings.weather_tz,
        )
    parquet = weather.json_to_parquet_bytes(payload)
    bytes_written = storage.put_bytes(bucket, key, parquet, content_type="application/x-parquet")

    daily = payload.get("daily", {})
    days = len(daily.get("time", []))
    dates = daily.get("time", [])
    date_min = dates[0] if dates else ""
    date_max = dates[-1] if dates else ""

    asset_materializations_total.labels(asset="bronze.weather_nyc_daily", status="ok").inc()
    rows_landed_total.labels(asset="bronze.weather_nyc_daily").inc(days)

    return MaterializeResult(
        metadata={
            "bucket": bucket,
            "key": key,
            "bytes": MetadataValue.int(bytes_written),
            "days": MetadataValue.int(days),
            "date_min": MetadataValue.text(date_min),
            "date_max": MetadataValue.text(date_max),
            "skipped": MetadataValue.bool(False),
        },
    )
