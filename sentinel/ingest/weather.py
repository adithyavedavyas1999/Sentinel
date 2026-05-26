"""Open-Meteo historical archive ingestion.

The free archive endpoint is generous but rate-limited. Be polite — fetch one
month at a time, daily granularity. NYC only for now; multi-city when we need
it.

Reference: https://open-meteo.com/en/docs/historical-weather-api
"""
from __future__ import annotations

import calendar
from datetime import date
from io import BytesIO
from typing import Any

import httpx
import polars as pl
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from sentinel.observability.logging import get_logger

log = get_logger(__name__)

_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Daily variables we pull. Keep this list short — wider columns mean larger
# payloads and we can always add more in a later iteration.
_DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
]


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TimeoutException | httpx.NetworkError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500 or exc.response.status_code == 429
    return False


def _month_bounds(year: int, month: int) -> tuple[date, date]:
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day)


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def _get_json(url: str, params: dict[str, Any], *, client: httpx.Client) -> dict[str, Any]:
    r = client.get(url, params=params)
    r.raise_for_status()
    return r.json()


def fetch_daily_archive(
    year: int,
    month: int,
    *,
    lat: float,
    lon: float,
    tz: str = "America/New_York",
    client: httpx.Client | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """Return the raw JSON payload for the given month at the given location."""
    start, end = _month_bounds(year, month)
    params: dict[str, Any] = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": ",".join(_DAILY_VARS),
        "timezone": tz,
    }

    own_client = client is None
    client = client or httpx.Client(timeout=timeout)
    try:
        log.info(
            "weather.fetch.start",
            year=year,
            month=month,
            lat=lat,
            lon=lon,
        )
        payload = _get_json(_ARCHIVE_URL, params, client=client)
        log.info("weather.fetch.ok", days=len(payload.get("daily", {}).get("time", [])))
        return payload
    finally:
        if own_client:
            client.close()


def json_to_parquet_bytes(payload: dict[str, Any]) -> bytes:
    """Flatten Open-Meteo's column-oriented JSON into a tidy DataFrame and
    write Parquet bytes.
    """
    daily = payload.get("daily")
    if not daily or "time" not in daily:
        raise ValueError("open-meteo payload missing 'daily.time'")

    # Open-Meteo returns each variable as a list aligned to daily.time.
    # We rebuild it row-wise into a polars DataFrame.
    columns: dict[str, list[Any]] = {"date": daily["time"]}
    for var in _DAILY_VARS:
        # missing variable -> all-null column; this happens when archive
        # doesn't have that var for the requested date range
        columns[var] = daily.get(var) or [None] * len(daily["time"])

    df = pl.DataFrame(columns).with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"),
    )

    buf = BytesIO()
    df.write_parquet(buf, compression="zstd")
    return buf.getvalue()
