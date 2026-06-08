from __future__ import annotations

from io import BytesIO

import polars as pl
import pytest

from sentinel import chaos
from sentinel.chaos import state as chaos_state


@pytest.fixture(autouse=True)
def _isolate_state(monkeypatch, tmp_path):
    monkeypatch.setenv("SENTINEL_CHAOS_STATE_PATH", str(tmp_path / "state.json"))


@pytest.fixture
def fake_chaos_storage(monkeypatch, fake_storage):
    """Patch sentinel.chaos._storage to return our in-memory FakeStorage."""
    monkeypatch.setattr(chaos, "_storage", lambda: fake_storage)
    fake_storage.ensure_bucket("sentinel-raw")
    return fake_storage


def _put_parquet(storage, bucket: str, key: str, df: pl.DataFrame) -> None:
    buf = BytesIO()
    df.write_parquet(buf, compression="zstd")
    storage.put_bytes(bucket, key, buf.getvalue())


def test_run_unknown_scenario_returns_nonzero():
    assert chaos.run("not_a_scenario") == 2


def test_dry_run_flag_scenarios_return_zero(fake_chaos_storage):
    """Flag scenarios always succeed in dry-run; the only side effect is logging."""
    for name in ("tlc_5xx", "weather_429", "duckdb_lock", "late_partition"):
        assert chaos.run(name, dry_run=True) == 0
    # and didn't actually set the flag
    assert chaos_state.list_active() == []


def test_dry_run_dbt_sql_error_does_not_write(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    assert chaos.run("dbt_sql_error", dry_run=True) == 0
    assert not (tmp_path / "dbt/models/_chaos/broken.sql").exists()


def test_dry_run_destructive_scenarios_with_data_dont_mutate(fake_chaos_storage):
    """Pre-seed data, dry-run, confirm bytes are unchanged."""
    tlc_df = pl.DataFrame(
        {
            "VendorID": [1],
            "tpep_pickup_datetime": ["2024-04-01 00:00:00"],
        }
    )
    weather_df = pl.DataFrame(
        {
            "date": ["2024-04-01"],
            "temperature_2m_mean": [55.0],
        }
    )
    tlc_key = "bronze/tlc/yellow/year=2024/month=04/yellow_tripdata_2024-04.parquet"
    weather_key = "bronze/weather/nyc/year=2024/month=04/weather_nyc_2024-04.parquet"
    _put_parquet(fake_chaos_storage, "sentinel-raw", tlc_key, tlc_df)
    _put_parquet(fake_chaos_storage, "sentinel-raw", weather_key, weather_df)

    tlc_before = fake_chaos_storage.get_bytes("sentinel-raw", tlc_key)
    weather_before = fake_chaos_storage.get_bytes("sentinel-raw", weather_key)

    for name in ("tlc_schema_drift", "weather_schema_change", "null_spike", "volume_drop"):
        assert chaos.run(name, dry_run=True) == 0

    assert fake_chaos_storage.get_bytes("sentinel-raw", tlc_key) == tlc_before
    assert fake_chaos_storage.get_bytes("sentinel-raw", weather_key) == weather_before


def test_state_flag_scenarios_set_flag(fake_chaos_storage):
    for name in ("tlc_5xx", "weather_429", "duckdb_lock", "late_partition"):
        chaos.run(name)
        assert chaos_state.is_active(name) is True


def test_clear_state_flag(fake_chaos_storage):
    chaos.run("tlc_5xx")
    assert chaos.clear("tlc_5xx") == 0
    assert chaos_state.is_active("tlc_5xx") is False


def test_clear_returns_nonzero_when_nothing_set():
    assert chaos.clear("tlc_5xx") == 1


def test_dbt_sql_error_writes_and_clears(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    rc = chaos.run("dbt_sql_error")
    assert rc == 0
    assert (tmp_path / "dbt/models/_chaos/broken.sql").exists()

    assert chaos.clear("dbt_sql_error") == 0
    assert not (tmp_path / "dbt/models/_chaos/broken.sql").exists()


def test_tlc_schema_drift_no_data(fake_chaos_storage):
    """With nothing in the bucket, schema-drift bails out cleanly."""
    rc = chaos.run("tlc_schema_drift")
    assert rc == 1


def test_tlc_schema_drift_renames_column(fake_chaos_storage):
    df = pl.DataFrame(
        {
            "VendorID": [1, 2],
            "tpep_pickup_datetime": ["2024-04-01 00:00:00", "2024-04-01 00:01:00"],
            "fare_amount": [12.5, 8.0],
        }
    )
    _put_parquet(
        fake_chaos_storage,
        "sentinel-raw",
        "bronze/tlc/yellow/year=2024/month=04/yellow_tripdata_2024-04.parquet",
        df,
    )

    rc = chaos.run("tlc_schema_drift")
    assert rc == 0

    after = pl.read_parquet(
        BytesIO(
            fake_chaos_storage.get_bytes(
                "sentinel-raw",
                "bronze/tlc/yellow/year=2024/month=04/yellow_tripdata_2024-04.parquet",
            )
        )
    )
    assert "VendorID" not in after.columns
    assert "vendor_id_renamed" in after.columns


def test_weather_schema_change_renames_column(fake_chaos_storage):
    df = pl.DataFrame(
        {
            "date": ["2024-04-01", "2024-04-02"],
            "temperature_2m_mean": [55.0, 58.0],
            "precipitation_sum": [0.0, 0.1],
        }
    )
    _put_parquet(
        fake_chaos_storage,
        "sentinel-raw",
        "bronze/weather/nyc/year=2024/month=04/weather_nyc_2024-04.parquet",
        df,
    )

    rc = chaos.run("weather_schema_change")
    assert rc == 0

    after = pl.read_parquet(
        BytesIO(
            fake_chaos_storage.get_bytes(
                "sentinel-raw",
                "bronze/weather/nyc/year=2024/month=04/weather_nyc_2024-04.parquet",
            )
        )
    )
    assert "temperature_2m_mean" not in after.columns
    assert "temp_2m_mean" in after.columns


def test_volume_drop_no_data(fake_chaos_storage):
    """No bronze TLC parquet present -> bail out cleanly with rc=1."""
    rc = chaos.run("volume_drop")
    assert rc == 1


def test_volume_drop_truncates_to_keep_fraction(fake_chaos_storage):
    df = pl.DataFrame(
        {
            "VendorID": list(range(100)),
            "tpep_pickup_datetime": [f"2024-04-{(i % 28) + 1:02d} 00:00:00" for i in range(100)],
            "fare_amount": [float(i) for i in range(100)],
        }
    )
    key = "bronze/tlc/yellow/year=2024/month=04/yellow_tripdata_2024-04.parquet"
    _put_parquet(fake_chaos_storage, "sentinel-raw", key, df)

    rc = chaos.run("volume_drop")
    assert rc == 0

    after = pl.read_parquet(BytesIO(fake_chaos_storage.get_bytes("sentinel-raw", key)))
    # _VOLUME_DROP_KEEP_FRACTION = 0.05 -> 5 rows from 100
    assert after.height == 5
    # head() preserves order, so rows 0..4 should be intact
    assert after["VendorID"].to_list() == [0, 1, 2, 3, 4]


def test_volume_drop_keeps_at_least_one_row(fake_chaos_storage):
    """Tiny inputs floor at one row, not zero."""
    df = pl.DataFrame(
        {
            "VendorID": [1, 2, 3],
            "tpep_pickup_datetime": ["2024-04-01 00:00:00"] * 3,
        }
    )
    key = "bronze/tlc/yellow/year=2024/month=04/yellow_tripdata_2024-04.parquet"
    _put_parquet(fake_chaos_storage, "sentinel-raw", key, df)

    assert chaos.run("volume_drop") == 0
    after = pl.read_parquet(BytesIO(fake_chaos_storage.get_bytes("sentinel-raw", key)))
    assert after.height == 1


def test_late_partition_sets_and_clears_flag(fake_chaos_storage):
    chaos.run("late_partition")
    assert chaos_state.is_active("late_partition") is True
    assert chaos.clear("late_partition") == 0
    assert chaos_state.is_active("late_partition") is False


def test_null_spike_inserts_nulls(fake_chaos_storage):
    df = pl.DataFrame(
        {
            "VendorID": list(range(40)),
            "tpep_pickup_datetime": [f"2024-04-{(i % 28) + 1:02d} 00:00:00" for i in range(40)],
        }
    )
    _put_parquet(
        fake_chaos_storage,
        "sentinel-raw",
        "bronze/tlc/yellow/year=2024/month=04/yellow_tripdata_2024-04.parquet",
        df,
    )

    rc = chaos.run("null_spike")
    assert rc == 0

    after = pl.read_parquet(
        BytesIO(
            fake_chaos_storage.get_bytes(
                "sentinel-raw",
                "bronze/tlc/yellow/year=2024/month=04/yellow_tripdata_2024-04.parquet",
            )
        )
    )
    null_count = after["tpep_pickup_datetime"].null_count()
    assert null_count > 0
    # mod-19 rule: 40 rows -> indices 0,19,38 are null = 3
    assert null_count == 3


def test_clear_all_clears_flags_and_files(fake_chaos_storage, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    chaos.run("tlc_5xx")
    chaos.run("weather_429")
    chaos.run("dbt_sql_error")

    assert chaos.clear_all() == 0
    assert chaos_state.is_active("tlc_5xx") is False
    assert chaos_state.is_active("weather_429") is False
    assert not (tmp_path / "dbt/models/_chaos/broken.sql").exists()
