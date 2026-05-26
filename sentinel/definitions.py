from __future__ import annotations

from dagster import Definitions, load_assets_from_package_module

from sentinel.assets import bronze, silver
from sentinel.assets.dbt_models import sentinel_dbt_assets
from sentinel.assets.silver.trips_weather import (
    check_pickup_ts_not_null,
    check_rowcount_positive,
)
from sentinel.observability.logging import configure_logging
from sentinel.observability.metrics import start_metrics_server
from sentinel.resources import ObjectStorage, dbt_resource
from sentinel.sensors import failure_capture_sensor, tlc_freshness_sensor
from sentinel.settings import get_settings

settings = get_settings()
configure_logging(level=settings.log_level, json=settings.log_json)
# /metrics on 9464 — see docker/prometheus/prometheus.yml scrape config
start_metrics_server()

bronze_assets = load_assets_from_package_module(bronze, group_name="bronze")
silver_assets = load_assets_from_package_module(silver, group_name="silver")

defs = Definitions(
    assets=[*bronze_assets, *silver_assets, sentinel_dbt_assets],
    asset_checks=[check_rowcount_positive, check_pickup_ts_not_null],
    sensors=[tlc_freshness_sensor, failure_capture_sensor],
    resources={
        "storage": ObjectStorage(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        ),
        "dbt": dbt_resource,
    },
)
