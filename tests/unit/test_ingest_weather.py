from __future__ import annotations

import io

import httpx
import polars as pl
import pytest
import respx

from sentinel.ingest import weather


def _fake_payload(days: int = 3) -> dict:
    times = [f"2024-01-{i + 1:02d}" for i in range(days)]
    return {
        "latitude": 40.7128,
        "longitude": -74.006,
        "daily": {
            "time": times,
            "temperature_2m_max": [4.5, 5.1, 3.2][:days],
            "temperature_2m_min": [-1.0, 0.0, -2.5][:days],
            "temperature_2m_mean": [1.5, 2.5, 0.0][:days],
            "precipitation_sum": [0.0, 1.2, 0.0][:days],
            "rain_sum": [0.0, 1.2, 0.0][:days],
            "snowfall_sum": [0.0, 0.0, 0.0][:days],
            "wind_speed_10m_max": [12.0, 18.0, 9.0][:days],
            "wind_gusts_10m_max": [20.0, 30.0, 15.0][:days],
        },
    }


@respx.mock
def test_fetch_daily_archive_passes_expected_params():
    route = respx.get("https://archive-api.open-meteo.com/v1/archive").mock(
        return_value=httpx.Response(200, json=_fake_payload())
    )
    out = weather.fetch_daily_archive(2024, 1, lat=40.7128, lon=-74.006, tz="America/New_York")
    assert route.called
    sent = route.calls[0].request.url
    assert "start_date=2024-01-01" in str(sent)
    assert "end_date=2024-01-31" in str(sent)
    assert "timezone=America%2FNew_York" in str(sent)
    assert out["daily"]["time"]


@respx.mock
def test_fetch_retries_5xx():
    route = respx.get("https://archive-api.open-meteo.com/v1/archive")
    route.side_effect = [
        httpx.Response(502),
        httpx.Response(200, json=_fake_payload()),
    ]
    weather.fetch_daily_archive(2024, 2, lat=40.7128, lon=-74.006, tz="America/New_York")
    assert route.call_count == 2


def test_json_to_parquet_roundtrip():
    parquet = weather.json_to_parquet_bytes(_fake_payload(days=3))
    df = pl.read_parquet(io.BytesIO(parquet))
    assert df.shape == (3, 9)
    assert df.columns[0] == "date"
    assert df["temperature_2m_max"].to_list() == [4.5, 5.1, 3.2]


def test_json_to_parquet_raises_on_empty_payload():
    with pytest.raises(ValueError, match="missing 'daily.time'"):
        weather.json_to_parquet_bytes({"daily": {}})


def test_json_to_parquet_tolerates_missing_variable():
    payload = _fake_payload(days=2)
    del payload["daily"]["snowfall_sum"]
    parquet = weather.json_to_parquet_bytes(payload)
    df = pl.read_parquet(io.BytesIO(parquet))
    assert df["snowfall_sum"].is_null().all()
