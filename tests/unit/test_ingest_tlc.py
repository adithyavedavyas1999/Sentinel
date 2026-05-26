from __future__ import annotations

import httpx
import pytest
import respx

from sentinel.ingest import tlc


def test_yellow_url_formats_zero_padded():
    assert tlc.yellow_url(2024, 1).endswith("/yellow_tripdata_2024-01.parquet")
    assert tlc.yellow_url(2024, 12).endswith("/yellow_tripdata_2024-12.parquet")


@respx.mock
def test_fetch_returns_bytes_on_200():
    body = b"PAR1" + b"\0" * 100
    respx.get(tlc.yellow_url(2024, 1)).mock(return_value=httpx.Response(200, content=body))
    out = tlc.fetch_yellow_tripdata(2024, 1)
    assert out == body


@respx.mock
def test_fetch_retries_on_503_then_succeeds():
    body = b"ok"
    route = respx.get(tlc.yellow_url(2024, 2))
    route.side_effect = [
        httpx.Response(503),
        httpx.Response(503),
        httpx.Response(200, content=body),
    ]
    out = tlc.fetch_yellow_tripdata(2024, 2)
    assert out == body
    assert route.call_count == 3


@respx.mock
def test_fetch_does_not_retry_on_404():
    respx.get(tlc.yellow_url(1999, 1)).mock(return_value=httpx.Response(404))
    with pytest.raises(httpx.HTTPStatusError) as exc:
        tlc.fetch_yellow_tripdata(1999, 1)
    assert exc.value.response.status_code == 404


@respx.mock
def test_fetch_retries_on_429():
    body = b"backed-off"
    route = respx.get(tlc.yellow_url(2024, 3))
    route.side_effect = [httpx.Response(429), httpx.Response(200, content=body)]
    out = tlc.fetch_yellow_tripdata(2024, 3)
    assert out == body
    assert route.call_count == 2
