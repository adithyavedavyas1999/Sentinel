from __future__ import annotations

import pytest

from sentinel.quality.incidents import Incident, IncidentStore


@pytest.fixture
def store(tmp_path):
    return IncidentStore(path=str(tmp_path / "incidents.sqlite"))


def test_insert_and_list_open(store):
    iid = store.insert(
        Incident(
            asset_key="bronze.tlc_yellow",
            error_type="HTTPStatusError",
            error_message="503",
        )
    )
    rows = store.list_open()
    assert len(rows) == 1
    assert rows[0]["id"] == iid
    assert rows[0]["status"] == "open"


def test_get_returns_full_record(store):
    iid = store.insert(
        Incident(
            asset_key="silver.trips_weather",
            error_type="ValueError",
            error_message="bad join",
            upstream_lineage=["bronze.tlc_yellow", "bronze.weather_nyc_daily"],
            recent_metadata={"run_id": "abc"},
        )
    )
    row = store.get(iid)
    assert row is not None
    assert row["upstream_lineage"] == '["bronze.tlc_yellow", "bronze.weather_nyc_daily"]'
    assert "abc" in row["recent_metadata"]


def test_resolve_moves_off_open_list(store):
    iid = store.insert(Incident(asset_key="x", error_type="t", error_message="m"))
    store.resolve(iid)
    assert store.list_open() == []
    row = store.get(iid)
    assert row["status"] == "resolved"
    assert row["resolved_at"] is not None


def test_get_returns_none_for_unknown_id(store):
    assert store.get("nope") is None
