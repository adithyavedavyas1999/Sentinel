from __future__ import annotations

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from sentinel.observability.logging import get_logger

log = get_logger(__name__)

# FIXME: TLC moved hosts at least once historically (from nyc.gov S3 to
# CloudFront). If this 404s in CI someday, check the TLC trip-data page first.
_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def yellow_url(year: int, month: int) -> str:
    return f"{_BASE_URL}/yellow_tripdata_{year:04d}-{month:02d}.parquet"


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TimeoutException | httpx.NetworkError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code >= 500 or code == 429
    return False


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def _get(url: str, *, client: httpx.Client) -> bytes:
    r = client.get(url)
    r.raise_for_status()
    return r.content


def fetch_yellow_tripdata(
    year: int,
    month: int,
    *,
    client: httpx.Client | None = None,
    timeout: float = 60.0,
) -> bytes:
    """Return the raw parquet bytes for a single (year, month).

    Retries on 5xx, 429, and transient network errors. Raises on 4xx.
    """
    url = yellow_url(year, month)
    own_client = client is None
    client = client or httpx.Client(timeout=timeout, follow_redirects=True)
    try:
        log.info("tlc.fetch.start", url=url, year=year, month=month)
        data = _get(url, client=client)
        log.info("tlc.fetch.ok", url=url, bytes=len(data))
        return data
    finally:
        if own_client:
            client.close()
