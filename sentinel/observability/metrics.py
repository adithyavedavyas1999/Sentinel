"""Prometheus metrics for the pipeline.

We expose a /metrics endpoint via prometheus_client's start_http_server, fired
once at process start. Dagster's webserver doesn't expose metrics natively
yet — instead of running a separate exporter we piggyback on a side HTTP
server on port 9464.

If we ever move to dagster-prometheus or otel, this module is the place to
rewrite.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager

from prometheus_client import Counter, Histogram, start_http_server

_started = False
_lock = threading.Lock()

# Labels are kept small on purpose — high-cardinality labels are the
# classic prometheus footgun.
asset_materializations_total = Counter(
    "sentinel_asset_materializations_total",
    "Asset materialization outcomes, partitioned by status.",
    ["asset", "status"],
)

ingest_latency_seconds = Histogram(
    "sentinel_ingest_latency_seconds",
    "Time to fetch a single bronze partition from its upstream source.",
    ["source"],
    buckets=(0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)

dq_check_total = Counter(
    "sentinel_dq_check_total",
    "Asset check outcomes by name and result.",
    ["check_name", "result"],
)

rows_landed_total = Counter(
    "sentinel_rows_landed_total",
    "Rows landed per bronze asset materialization.",
    ["asset"],
)


def start_metrics_server(port: int | None = None) -> None:
    """Start the prometheus exporter on first call; no-op afterwards."""
    global _started
    with _lock:
        if _started:
            return
        port = port or int(os.environ.get("SENTINEL_METRICS_PORT", "9464"))
        start_http_server(port)
        _started = True


@contextmanager
def time_ingest(source: str) -> Iterator[None]:
    """Context manager that records ingest latency on exit, success or not."""
    t0 = time.perf_counter()
    try:
        yield
    finally:
        ingest_latency_seconds.labels(source=source).observe(time.perf_counter() - t0)
