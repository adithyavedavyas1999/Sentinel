"""dbt models surfaced as Dagster assets via dagster-dbt."""

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from sentinel.chaos.state import ChaosTriggered
from sentinel.chaos.state import is_active as chaos_active
from sentinel.resources.dbt import dbt_project


@dbt_assets(manifest=dbt_project.manifest_path)
def sentinel_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    # The "lock" is simulated; we don't actually grab a duckdb file lock.
    # The agent diagnostic should still classify this correctly given the
    # error class + asset key + recent metadata.
    if chaos_active("duckdb_lock"):
        raise ChaosTriggered("chaos:duckdb_lock — simulated warehouse lock contention")
    yield from dbt.cli(["build"], context=context).stream()
