"""Chaos injection harness.

Nine scenarios, all wired end-to-end as of the week-7 wrap-up. Originally
shipped six and left ``volume_drop`` / ``late_partition`` as stubs pending
"future infra"; that turned out to be over-cautious. The injections
themselves don't need historical baselines or partition sensors — those
live downstream in the agent (week 10), not here. So both got implemented
the same week.

Two flavors of scenario:

- **State-flag scenarios** (`tlc_5xx`, `weather_429`, `duckdb_lock`,
  `late_partition`) leave a marker in ``data/chaos/state.json``. Assets
  read the marker on materialization and raise
  :class:`~sentinel.chaos.state.ChaosTriggered`. Reversed by
  ``sentinel chaos clear <name>``.

- **Destructive scenarios** (`tlc_schema_drift`, `weather_schema_change`,
  `null_spike`, `dbt_sql_error`, `volume_drop`) mutate artifacts on disk
  or in MinIO. `dbt_sql_error` is the only one with a clean reverse —
  the others require re-running the relevant ingest to restore good data.

The agent's job in phase 2: read the failure, infer which scenario caused
it (by the error class + asset key + recent metadata), pick a remediation
from the allowlist (week 10), and clear the matching flag if the retry
succeeds.
"""

from __future__ import annotations

from collections.abc import Callable
from io import BytesIO
from pathlib import Path

import polars as pl

from sentinel.chaos import state as chaos_state
from sentinel.observability.logging import get_logger
from sentinel.resources import ObjectStorage
from sentinel.settings import get_settings

log = get_logger(__name__)

# Where dbt_sql_error drops its broken file. Lives outside the regular
# models dir so it's a tidy diff to inspect, but on the dbt model path
# so dbt actually picks it up.
_BROKEN_DBT_SQL_PATH = Path("dbt/models/_chaos/broken.sql")
_BROKEN_DBT_SQL = """-- chaos:dbt_sql_error
-- this model is intentionally broken so we can exercise the dbt-failure
-- path. delete this file (or run `sentinel chaos clear dbt_sql_error`)
-- to restore.
select 1 as id
from {{ ref('definitely_not_a_real_model') }}
where this_column_does_not_exist
"""


def _storage() -> ObjectStorage:
    s = get_settings()
    return ObjectStorage(
        endpoint=s.minio_endpoint,
        access_key=s.minio_access_key,
        secret_key=s.minio_secret_key,
        secure=s.minio_secure,
    )


def _latest_key(prefix: str) -> str | None:
    s = get_settings()
    keys = list(_storage().list_keys(s.bucket_bronze, prefix))
    if not keys:
        return None
    return sorted(keys)[-1]


def _read_parquet(bucket: str, key: str) -> pl.DataFrame:
    return pl.read_parquet(BytesIO(_storage().get_bytes(bucket, key)))


def _write_parquet(bucket: str, key: str, df: pl.DataFrame) -> None:
    buf = BytesIO()
    df.write_parquet(buf, compression="zstd")
    _storage().put_bytes(
        bucket,
        key,
        buf.getvalue(),
        content_type="application/x-parquet",
    )


# --- destructive scenarios --------------------------------------------------


def tlc_schema_drift(dry_run: bool) -> int:
    """Rename `VendorID` on the most recent TLC bronze parquet.

    Real TLC has done exactly this — e.g. when they renamed `VendorID`
    casing. Should trip stg_tlc_yellow's schema fingerprint check and
    eventually the dbt staging compile.
    """
    s = get_settings()
    target = _latest_key("bronze/tlc/yellow/")
    if target is None:
        log.warning("chaos.tlc_schema_drift.no_data")
        return 1
    log.info("chaos.tlc_schema_drift", target=target, dry_run=dry_run)
    if dry_run:
        return 0

    df = _read_parquet(s.bucket_bronze, target)
    if "VendorID" not in df.columns:
        log.warning("chaos.tlc_schema_drift.no_vendor_id_column")
        return 1
    df = df.rename({"VendorID": "vendor_id_renamed"})
    _write_parquet(s.bucket_bronze, target, df)
    return 0


def weather_schema_change(dry_run: bool) -> int:
    """Rename `temperature_2m_mean` on the most recent weather bronze.

    Open-Meteo has actually shipped renames before (around the v1 -> v1
    transition for some derived fields). The agent should detect a missing
    expected column rather than a wrong value range.
    """
    s = get_settings()
    target = _latest_key("bronze/weather/nyc/")
    if target is None:
        log.warning("chaos.weather_schema_change.no_data")
        return 1
    log.info("chaos.weather_schema_change", target=target, dry_run=dry_run)
    if dry_run:
        return 0

    df = _read_parquet(s.bucket_bronze, target)
    if "temperature_2m_mean" not in df.columns:
        log.warning("chaos.weather_schema_change.no_expected_column")
        return 1
    df = df.rename({"temperature_2m_mean": "temp_2m_mean"})
    _write_parquet(s.bucket_bronze, target, df)
    return 0


def null_spike(dry_run: bool) -> int:
    """Null out roughly 5% of pickup_ts on the latest TLC parquet.

    Should trip dbt not_null on stg_tlc_yellow.pickup_ts. The exact null
    rate is intentionally a little fuzzy — not aiming for a constant
    fixture, aiming for "this looks plausible-ish."
    """
    s = get_settings()
    target = _latest_key("bronze/tlc/yellow/")
    if target is None:
        log.warning("chaos.null_spike.no_data")
        return 1
    log.info("chaos.null_spike", target=target, dry_run=dry_run)
    if dry_run:
        return 0

    df = _read_parquet(s.bucket_bronze, target)
    pickup_col = next(
        (c for c in ("tpep_pickup_datetime", "pickup_datetime") if c in df.columns),
        None,
    )
    if pickup_col is None:
        log.warning("chaos.null_spike.no_pickup_column")
        return 1

    n = df.height
    # mod-19 picks ~5.3% of rows. deterministic per row, no rng needed.
    mask = pl.int_range(0, n, eager=True) % 19 == 0
    df = df.with_columns(
        pl.when(mask).then(None).otherwise(pl.col(pickup_col)).alias(pickup_col),
    )
    _write_parquet(s.bucket_bronze, target, df)
    return 0


def dbt_sql_error(dry_run: bool) -> int:
    """Drop a syntactically-broken SQL file into the dbt model tree.

    Cleared by `sentinel chaos clear dbt_sql_error` (which deletes the file).
    """
    log.info("chaos.dbt_sql_error", path=str(_BROKEN_DBT_SQL_PATH), dry_run=dry_run)
    if dry_run:
        return 0
    _BROKEN_DBT_SQL_PATH.parent.mkdir(parents=True, exist_ok=True)
    _BROKEN_DBT_SQL_PATH.write_text(_BROKEN_DBT_SQL)
    return 0


# Volume-drop keeps the first ~5% of rows. Picked the rate to match
# docs/chaos-scenarios.md so the agent's row-count tolerance check has a
# clearly anomalous (but not zero-row) input to react to.
_VOLUME_DROP_KEEP_FRACTION = 0.05


def volume_drop(dry_run: bool) -> int:
    """Truncate the latest TLC bronze parquet to the first ~5% of rows.

    Real TLC has shipped occasional partial publishes — a partition appears
    on schedule but with only the first few hours of trips. The agent's
    expected response (week 11) is *not* to auto-heal; row-count
    anomalies are ambiguous (holiday vs. real bug) and need a human.
    Here we just produce the failure mode.
    """
    s = get_settings()
    target = _latest_key("bronze/tlc/yellow/")
    if target is None:
        log.warning("chaos.volume_drop.no_data")
        return 1
    log.info("chaos.volume_drop", target=target, dry_run=dry_run)
    if dry_run:
        return 0

    df = _read_parquet(s.bucket_bronze, target)
    if df.height == 0:
        log.warning("chaos.volume_drop.empty_input", target=target)
        return 1
    keep = max(1, int(df.height * _VOLUME_DROP_KEEP_FRACTION))
    df = df.head(keep)
    _write_parquet(s.bucket_bronze, target, df)
    return 0


# --- state-flag scenarios ---------------------------------------------------


def _flag(name: str) -> Callable[[bool], int]:
    def _impl(dry_run: bool) -> int:
        log.info(f"chaos.{name}", dry_run=dry_run)
        if dry_run:
            return 0
        chaos_state.set_active(name)
        return 0

    return _impl


_REGISTRY: dict[str, Callable[[bool], int]] = {
    "tlc_5xx": _flag("tlc_5xx"),
    "tlc_schema_drift": tlc_schema_drift,
    "weather_429": _flag("weather_429"),
    "weather_schema_change": weather_schema_change,
    "duckdb_lock": _flag("duckdb_lock"),
    "dbt_sql_error": dbt_sql_error,
    "null_spike": null_spike,
    "volume_drop": volume_drop,
    "late_partition": _flag("late_partition"),
}


def scenarios() -> list[str]:
    return list(_REGISTRY.keys())


def run(scenario: str, *, dry_run: bool = False) -> int:
    fn = _REGISTRY.get(scenario)
    if fn is None:
        log.error("chaos.unknown_scenario", scenario=scenario)
        return 2
    return fn(dry_run)


def clear(scenario: str) -> int:
    """Reverse a scenario where possible.

    State-flag scenarios get their flag dropped. `dbt_sql_error` deletes
    its file. Destructive parquet rewrites are not auto-undone — you re-run
    the matching ingest partition to restore good data.
    """
    if scenario == "dbt_sql_error":
        if _BROKEN_DBT_SQL_PATH.exists():
            _BROKEN_DBT_SQL_PATH.unlink()
            log.info("chaos.cleared", scenario=scenario)
            return 0
        return 1
    if chaos_state.clear(scenario):
        log.info("chaos.cleared", scenario=scenario)
        return 0
    log.warning("chaos.clear.nothing_to_clear", scenario=scenario)
    return 1


def clear_all() -> int:
    n = chaos_state.clear_all()
    if _BROKEN_DBT_SQL_PATH.exists():
        _BROKEN_DBT_SQL_PATH.unlink()
        n += 1
    log.info("chaos.clear_all", count=n)
    return 0


if __name__ == "__main__":
    import sys

    raise SystemExit(run(sys.argv[1], dry_run="--dry-run" in sys.argv))
