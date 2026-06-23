"""Unit tests for the allowlisted remediation actions.

Each scenario uses fixture-style inputs (the same dict shape the
agent produces in ``incident_report``) so the tests are also the
de-facto contract for what an action will see at runtime.
"""

from __future__ import annotations

from io import BytesIO

import polars as pl
import pytest

from sentinel.agent.remediation import (
    allowlist,
    dispatch,
    get_action,
)
from sentinel.agent.remediation.coerce_to_string import (
    CoerceToStringAction,
    _quarantine_key,
)
from sentinel.agent.remediation.partition_window_slip import (
    PartitionWindowSlipAction,
    _slip_one_month,
)
from sentinel.agent.remediation.retry_with_backoff import RetryWithBackoffAction
from sentinel.agent.remediation.types import RemediationDeps


class StubChaosState:
    """Minimal stand-in for sentinel.chaos.state.

    Same contract as the real module: ``clear`` returns True iff something
    was actually removed; ``set_active`` is no-arg. We don't bother with
    timestamps/payload.
    """

    def __init__(self) -> None:
        self.active: set[str] = set()
        self.cleared_calls: list[str] = []

    def set_active(self, name: str) -> None:
        self.active.add(name)

    def clear(self, name: str) -> bool:
        self.cleared_calls.append(name)
        existed = name in self.active
        self.active.discard(name)
        return existed


# ---------------------------------------------------------------------------
# registry + dispatch
# ---------------------------------------------------------------------------


def test_allowlist_matches_diagnose_module():
    from sentinel.agent.diagnose import ALLOWLISTED_FIXES

    assert set(allowlist()) == ALLOWLISTED_FIXES


def test_get_action_returns_none_for_unknown():
    assert get_action("rm-rf-prod") is None


def test_dispatch_returns_none_off_allowlist():
    incident = {"id": "x", "asset_key": "bronze/y", "partition_key": "2024-04-01"}
    assert dispatch("not-an-action", incident, RemediationDeps()) is None


def test_dispatch_returns_none_when_guard_fails():
    # Off-asset incident; retry-with-backoff guard rejects.
    incident = {"id": "x", "asset_key": "silver/trips", "partition_key": "2024-04-01"}
    assert dispatch("retry-with-backoff", incident, RemediationDeps()) is None


def test_dispatch_swallows_execute_exception(monkeypatch):
    """If execute() raises, dispatch returns None instead of propagating.

    The sensor treats None as 'don't remediate', so a buggy action can't
    blow up the whole sensor tick.
    """
    from sentinel.agent.remediation import _REGISTRY

    class ExplodingAction:
        name = "retry-with-backoff"

        def guard(self, incident):
            return True

        def execute(self, incident, deps):
            raise RuntimeError("intentional")

        def rollback(self, result, deps):
            pass

    monkeypatch.setitem(_REGISTRY, "retry-with-backoff", ExplodingAction())
    result = dispatch(
        "retry-with-backoff",
        {"id": "x", "asset_key": "bronze/tlc_yellow", "partition_key": "2024-04-01"},
        RemediationDeps(),
    )
    assert result is None


# ---------------------------------------------------------------------------
# retry-with-backoff
# ---------------------------------------------------------------------------


@pytest.fixture
def retry_action() -> RetryWithBackoffAction:
    return RetryWithBackoffAction()


def test_retry_guard_accepts_chaos_tlc_5xx(retry_action):
    incident = {
        "asset_key": "bronze/tlc_yellow",
        "partition_key": "2024-04-01",
        "error_type": "ChaosTriggered",
        "error_message": "chaos:tlc_5xx",
    }
    assert retry_action.guard(incident) is True


def test_retry_guard_rejects_silver_asset(retry_action):
    incident = {
        "asset_key": "silver/trips_weather",
        "partition_key": "2024-04-01",
        "error_type": "HTTPStatusError",
        "error_message": "503",
    }
    assert retry_action.guard(incident) is False


def test_retry_guard_rejects_unpartitioned(retry_action):
    incident = {
        "asset_key": "bronze/tlc_yellow",
        "partition_key": None,
        "error_type": "HTTPStatusError",
        "error_message": "503",
    }
    assert retry_action.guard(incident) is False


def test_retry_clears_tlc_5xx_flag(retry_action):
    chaos = StubChaosState()
    chaos.active.add("tlc_5xx")
    incident = {
        "id": "inc-1",
        "asset_key": "bronze/tlc_yellow",
        "partition_key": "2024-04-01",
        "error_type": "ChaosTriggered",
        "error_message": "chaos:tlc_5xx",
    }
    deps = RemediationDeps(chaos_state=chaos)
    result = retry_action.execute(incident, deps)

    assert result.success is True
    assert result.next_run is not None
    assert result.next_run.asset_key == "bronze/tlc_yellow"
    assert result.next_run.partition_key == "2024-04-01"
    assert result.next_run.run_tags["sentinel.remediation"] == "retry-with-backoff"
    assert result.next_run.run_tags["sentinel.cleared_flag"] == "tlc_5xx"
    assert "tlc_5xx" not in chaos.active


def test_retry_no_flag_still_emits_next_run(retry_action):
    incident = {
        "id": "inc-2",
        "asset_key": "bronze/tlc_yellow",
        "partition_key": "2024-04-01",
        "error_type": "HTTPStatusError",  # real 5xx, not chaos
        "error_message": "503 from CloudFront",
    }
    deps = RemediationDeps(chaos_state=StubChaosState())
    result = retry_action.execute(incident, deps)
    assert result.success is True
    assert result.next_run is not None
    assert "sentinel.cleared_flag" not in result.next_run.run_tags
    # tlc_yellow matches the _FLAG_MAP fallback -> tlc_5xx, so the
    # cleared_flag rollback note reflects the attempted clear even
    # though nothing was actually active.
    assert result.rollback_data["cleared_flag"] is None


def test_retry_rollback_resets_flag(retry_action):
    chaos = StubChaosState()
    chaos.active.add("tlc_5xx")
    incident = {
        "id": "inc-1",
        "asset_key": "bronze/tlc_yellow",
        "partition_key": "2024-04-01",
        "error_type": "ChaosTriggered",
        "error_message": "chaos:tlc_5xx",
    }
    deps = RemediationDeps(chaos_state=chaos)
    result = retry_action.execute(incident, deps)
    assert "tlc_5xx" not in chaos.active
    retry_action.rollback(result, deps)
    assert "tlc_5xx" in chaos.active


# ---------------------------------------------------------------------------
# partition-window-slip
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "given,expected",
    [
        ("2024-02-01", "2024-01-01"),
        ("2024-01-15", "2023-12-01"),  # year rollover + day normalize
        ("2025-03-31", "2025-02-01"),
    ],
)
def test_slip_one_month(given, expected):
    assert _slip_one_month(given) == expected


def test_slip_raises_on_garbage():
    with pytest.raises(ValueError, match="unparseable"):
        _slip_one_month("not-a-date")


def test_partition_slip_guard_requires_late_partition_signal():
    action = PartitionWindowSlipAction()
    # not late_partition shaped -> reject
    assert (
        action.guard(
            {
                "asset_key": "bronze/tlc_yellow",
                "partition_key": "2024-04-01",
                "error_type": "ValueError",
                "error_message": "bad column",
            }
        )
        is False
    )
    # late_partition flag -> accept
    assert (
        action.guard(
            {
                "asset_key": "bronze/tlc_yellow",
                "partition_key": "2024-04-01",
                "error_type": "ChaosTriggered",
                "error_message": "chaos:late_partition -- partition not yet published upstream (404)",
            }
        )
        is True
    )


def test_partition_slip_executes_and_tags():
    action = PartitionWindowSlipAction()
    chaos = StubChaosState()
    chaos.active.add("late_partition")
    incident = {
        "id": "inc-3",
        "asset_key": "bronze/tlc_yellow",
        "partition_key": "2024-04-01",
        "error_type": "ChaosTriggered",
        "error_message": "late_partition: 404",
    }
    deps = RemediationDeps(chaos_state=chaos)
    result = action.execute(incident, deps)
    assert result.success is True
    assert result.next_run.partition_key == "2024-03-01"
    assert result.next_run.run_tags["sentinel.partition_slipped_from"] == "2024-04-01"
    assert "late_partition" not in chaos.active


def test_partition_slip_rollback_is_noop():
    action = PartitionWindowSlipAction()
    # Should not raise, should not mutate.
    action.rollback(
        type("R", (), {"rollback_data": {"original_partition": "2024-04-01"}})(),  # type: ignore[arg-type]
        RemediationDeps(),
    )


# ---------------------------------------------------------------------------
# coerce-to-string
# ---------------------------------------------------------------------------


def _write_tlc_parquet(storage, bucket: str, key: str) -> None:
    df = pl.DataFrame(
        {
            "VendorID": [1, 2, 3],
            "tpep_pickup_datetime": ["2024-04-01 00:00:00", "2024-04-01 01:00:00", None],
            "fare_amount": [10.5, 20.0, 7.25],
        }
    )
    buf = BytesIO()
    df.write_parquet(buf, compression="zstd")
    storage.put_bytes(bucket, key, buf.getvalue(), content_type="application/x-parquet")


def test_quarantine_key_uses_bronze_prefix():
    assert (
        _quarantine_key("bronze/tlc/yellow/year=2024/month=04/yellow_tripdata_2024-04.parquet")
        == "bronze/_quarantine/tlc/yellow/year=2024/month=04/yellow_tripdata_2024-04.parquet"
    )
    assert _quarantine_key("other/x.parquet") == "bronze/_quarantine/other/x.parquet"


def test_coerce_guard_only_on_schema_signals():
    action = CoerceToStringAction()
    assert (
        action.guard(
            {
                "asset_key": "bronze/tlc_yellow",
                "error_type": "HTTPStatusError",
                "error_message": "503",
            }
        )
        is False
    )
    assert (
        action.guard(
            {
                "asset_key": "bronze/tlc_yellow",
                "error_type": "ValueError",
                "error_message": "no pickup datetime column found; got ['vendor_id', ...]",
            }
        )
        is True
    )


def test_coerce_writes_quarantine_and_emits_next_run(fake_storage):
    bucket = "sentinel-raw"
    key = "bronze/tlc/yellow/year=2024/month=04/yellow_tripdata_2024-04.parquet"
    _write_tlc_parquet(fake_storage, bucket, key)

    deps = RemediationDeps(storage=fake_storage, bucket_bronze=bucket)
    action = CoerceToStringAction()
    incident = {
        "id": "inc-4",
        "asset_key": "bronze/tlc_yellow",
        "partition_key": "2024-04-01",
        "error_type": "ValueError",
        "error_message": "schema drift on column 'vendor_id'",
    }
    result = action.execute(incident, deps)
    assert result.success is True
    qkey = _quarantine_key(key)
    assert fake_storage.object_exists(bucket, qkey)
    df = pl.read_parquet(BytesIO(fake_storage.get_bytes(bucket, qkey)))
    # All columns are utf8 after coercion
    assert all(t == pl.Utf8 for t in df.dtypes)
    assert result.next_run.partition_key == "2024-04-01"
    assert result.rollback_data["quarantine_key"] == qkey


def test_coerce_rollback_deletes_quarantine(fake_storage):
    bucket = "sentinel-raw"
    key = "bronze/tlc/yellow/year=2024/month=04/yellow_tripdata_2024-04.parquet"
    _write_tlc_parquet(fake_storage, bucket, key)

    deps = RemediationDeps(storage=fake_storage, bucket_bronze=bucket)
    action = CoerceToStringAction()
    result = action.execute(
        {
            "id": "inc-4",
            "asset_key": "bronze/tlc_yellow",
            "partition_key": "2024-04-01",
            "error_type": "ValueError",
            "error_message": "schema drift",
        },
        deps,
    )
    qkey = result.rollback_data["quarantine_key"]
    assert fake_storage.object_exists(bucket, qkey)
    action.rollback(result, deps)
    assert not fake_storage.object_exists(bucket, qkey)


def test_coerce_fails_gracefully_when_no_bronze_data(fake_storage):
    deps = RemediationDeps(storage=fake_storage, bucket_bronze="sentinel-raw")
    action = CoerceToStringAction()
    result = action.execute(
        {
            "id": "inc-5",
            "asset_key": "bronze/tlc_yellow",
            "partition_key": "2024-04-01",
            "error_type": "ValueError",
            "error_message": "schema drift",
        },
        deps,
    )
    assert result.success is False
    assert "could not locate" in result.description


def test_coerce_fails_when_no_storage_deps():
    """Without a storage handle there's nothing the action can do."""
    deps = RemediationDeps(storage=None)
    action = CoerceToStringAction()
    result = action.execute(
        {
            "id": "x",
            "asset_key": "bronze/tlc_yellow",
            "partition_key": "2024-04-01",
            "error_type": "ValueError",
            "error_message": "schema drift",
        },
        deps,
    )
    assert result.success is False
    assert "no storage available" in result.description


def test_coerce_unrecognized_asset_skips(fake_storage):
    """An asset that's neither tlc nor weather can't be located -> bail."""
    deps = RemediationDeps(storage=fake_storage, bucket_bronze="sentinel-raw")
    action = CoerceToStringAction()
    result = action.execute(
        {
            "id": "x",
            "asset_key": "bronze/other_asset",
            "partition_key": "2024-04-01",
            "error_type": "ValueError",
            "error_message": "schema drift on column foo",
        },
        deps,
    )
    assert result.success is False
    assert "could not locate" in result.description


def test_coerce_rollback_noop_without_storage():
    """Rollback w/o storage is a quiet no-op (not a crash)."""
    from sentinel.agent.remediation.types import RemediationResult

    action = CoerceToStringAction()
    # Should not raise
    action.rollback(
        RemediationResult(
            action="coerce-to-string",
            description="x",
            success=True,
            rollback_data={"quarantine_key": "k", "bucket": "b"},
        ),
        RemediationDeps(storage=None),
    )
