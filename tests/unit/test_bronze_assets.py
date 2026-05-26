from __future__ import annotations

import httpx
import respx
from dagster import build_asset_context

from sentinel.assets.bronze.tlc import bronze_tlc_yellow
from sentinel.assets.bronze.weather import bronze_weather_nyc_daily
from sentinel.ingest import tlc as tlc_ingest
from tests.unit.conftest import _STORE


def _ctx(storage, partition: str = "2024-01-01"):
    return build_asset_context(partition_key=partition, resources={"storage": storage})


@respx.mock
def test_bronze_tlc_yellow_lands_object(fake_storage):
    respx.get(tlc_ingest.yellow_url(2024, 1)).mock(
        return_value=httpx.Response(200, content=b"PAR1-payload")
    )

    result = bronze_tlc_yellow(_ctx(fake_storage, "2024-01-01"))

    assert result.metadata["skipped"].value is False
    expected_key = "bronze/tlc/yellow/year=2024/month=01/yellow_tripdata_2024-01.parquet"
    assert expected_key in _STORE["sentinel-raw"]


@respx.mock
def test_bronze_tlc_yellow_is_idempotent(fake_storage):
    url = tlc_ingest.yellow_url(2024, 2)
    route = respx.get(url).mock(return_value=httpx.Response(200, content=b"once"))

    bronze_tlc_yellow(_ctx(fake_storage, "2024-02-01"))
    second = bronze_tlc_yellow(_ctx(fake_storage, "2024-02-01"))

    assert second.metadata["skipped"].value is True
    assert route.call_count == 1


@respx.mock
def test_bronze_weather_lands_object(fake_storage):
    respx.get("https://archive-api.open-meteo.com/v1/archive").mock(
        return_value=httpx.Response(
            200,
            json={
                "daily": {
                    "time": ["2024-01-01", "2024-01-02"],
                    "temperature_2m_max": [4.5, 5.1],
                    "temperature_2m_min": [-1.0, 0.0],
                    "temperature_2m_mean": [1.5, 2.5],
                    "precipitation_sum": [0.0, 1.2],
                    "rain_sum": [0.0, 1.2],
                    "snowfall_sum": [0.0, 0.0],
                    "wind_speed_10m_max": [12.0, 18.0],
                    "wind_gusts_10m_max": [20.0, 30.0],
                }
            },
        )
    )

    result = bronze_weather_nyc_daily(_ctx(fake_storage, "2024-01-01"))
    assert result.metadata["skipped"].value is False
    assert result.metadata["days"].value == 2
    assert (
        "bronze/weather/nyc/year=2024/month=01/weather_nyc_2024-01.parquet"
        in _STORE["sentinel-raw"]
    )
