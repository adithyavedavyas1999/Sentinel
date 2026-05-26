from __future__ import annotations

import json

from typer.testing import CliRunner

from sentinel.cli import app
from sentinel.quality.incidents import Incident, IncidentStore

runner = CliRunner()


def test_chaos_list_outputs_scenarios():
    result = runner.invoke(app, ["chaos", "list"])
    assert result.exit_code == 0
    assert "tlc_schema_drift" in result.stdout
    assert "null_spike" in result.stdout


def test_chaos_inject_rejects_unknown(monkeypatch):
    result = runner.invoke(app, ["chaos", "inject", "no_such_scenario"])
    assert result.exit_code != 0
    assert "unknown scenario" in result.stdout + result.stderr


def test_incident_list_empty(monkeypatch, tmp_path):
    monkeypatch.setenv("SENTINEL_INCIDENTS_DB", str(tmp_path / "i.sqlite"))
    # bootstrap an empty store so the file exists
    IncidentStore()
    result = runner.invoke(app, ["incident", "list"])
    assert result.exit_code == 0
    assert "no open incidents" in result.stdout


def test_incident_list_shows_inserted(monkeypatch, tmp_path):
    monkeypatch.setenv("SENTINEL_INCIDENTS_DB", str(tmp_path / "i.sqlite"))
    s = IncidentStore()
    s.insert(Incident(asset_key="bronze.x", error_type="E", error_message="boom"))
    result = runner.invoke(app, ["incident", "list", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert len(data) == 1
    assert data[0]["asset_key"] == "bronze.x"


def test_incident_show_missing_returns_nonzero(monkeypatch, tmp_path):
    monkeypatch.setenv("SENTINEL_INCIDENTS_DB", str(tmp_path / "i.sqlite"))
    IncidentStore()
    result = runner.invoke(app, ["incident", "show", "does-not-exist"])
    assert result.exit_code == 1
