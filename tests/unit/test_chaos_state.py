from __future__ import annotations

import json

import pytest

from sentinel.chaos import state


@pytest.fixture(autouse=True)
def _isolate_state_file(monkeypatch, tmp_path):
    monkeypatch.setenv("SENTINEL_CHAOS_STATE_PATH", str(tmp_path / "state.json"))


def test_no_state_file_means_inactive():
    assert state.is_active("tlc_5xx") is False
    assert state.list_active() == []


def test_set_then_active():
    state.set_active("tlc_5xx", reason="testing")
    assert state.is_active("tlc_5xx") is True
    rows = state.list_active()
    assert len(rows) == 1
    assert rows[0]["name"] == "tlc_5xx"
    assert rows[0]["payload"]["reason"] == "testing"


def test_clear_returns_false_when_nothing_to_clear():
    assert state.clear("never_set") is False


def test_clear_returns_true_after_set():
    state.set_active("weather_429")
    assert state.clear("weather_429") is True
    assert state.is_active("weather_429") is False


def test_clear_all():
    state.set_active("a")
    state.set_active("b")
    n = state.clear_all()
    assert n == 2
    assert state.list_active() == []


def test_clear_all_when_empty_is_zero():
    assert state.clear_all() == 0


def test_malformed_state_file_is_treated_as_empty(monkeypatch, tmp_path):
    p = tmp_path / "broken.json"
    p.write_text("{not valid json")
    monkeypatch.setenv("SENTINEL_CHAOS_STATE_PATH", str(p))
    assert state.is_active("tlc_5xx") is False
    # writing a flag should silently overwrite the broken file
    state.set_active("tlc_5xx")
    parsed = json.loads(p.read_text())
    assert "tlc_5xx" in parsed


def test_chaos_triggered_is_runtime_error():
    assert issubclass(state.ChaosTriggered, RuntimeError)
