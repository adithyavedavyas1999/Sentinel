from __future__ import annotations

from sentinel.chaos import run as run_scenario


def test_run_unknown_scenario_returns_nonzero():
    assert run_scenario("not_a_scenario") == 2


def test_dry_run_todo_scenarios_return_zero():
    for s in (
        "tlc_5xx",
        "weather_429",
        "duckdb_lock",
        "dbt_sql_error",
        "volume_drop",
        "late_partition",
    ):
        assert run_scenario(s, dry_run=True) == 0
