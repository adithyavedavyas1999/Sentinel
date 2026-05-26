"""dbt models surfaced as Dagster assets via dagster-dbt."""
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from sentinel.resources.dbt import dbt_project


@dbt_assets(manifest=dbt_project.manifest_path)
def sentinel_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
