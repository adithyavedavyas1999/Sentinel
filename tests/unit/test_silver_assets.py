from __future__ import annotations

from io import BytesIO

import polars as pl
from dagster import build_asset_context

from sentinel.assets.silver.trips_weather import (
    _bronze_tlc_key,
    _bronze_weather_key,
    _silver_key,
    silver_trips_weather,
)
from tests.unit.conftest import _STORE


def _put(storage, key: str, df: pl.DataFrame) -> None:
    buf = BytesIO()
    df.write_parquet(buf)
    storage.put_bytes("sentinel-raw", key, buf.getvalue())


def test_silver_joins_on_pickup_date(fake_storage):
    year, month = 2024, 1
    trips = pl.DataFrame(
        {
            "tpep_pickup_datetime": [
                "2024-01-01 10:00:00",
                "2024-01-02 11:30:00",
                "2024-01-02 12:00:00",
            ],
            "fare_amount": [10.5, 12.0, 8.75],
        }
    ).with_columns(pl.col("tpep_pickup_datetime").str.to_datetime())

    weather = pl.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "temperature_2m_max": [5.0, 7.0],
        }
    ).with_columns(pl.col("date").str.to_date())

    _put(fake_storage, _bronze_tlc_key(year, month), trips)
    _put(fake_storage, _bronze_weather_key(year, month), weather)

    # FakeStorage doesn't support get_object via _client(); patch it inline
    # by stuffing a tiny adapter. Real flow goes through MinIO.
    import sentinel.assets.silver.trips_weather as silver_mod

    def _read(storage, bucket, key):
        return _STORE[bucket][key]

    silver_mod._read_object = _read

    ctx = build_asset_context(partition_key="2024-01-01", resources={"storage": fake_storage})
    result = silver_trips_weather(ctx, _tlc=None, _weather=None)

    assert result.metadata["rows"].value == 3
    assert _silver_key(year, month) in _STORE["sentinel-raw"]
