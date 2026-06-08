"""Materialize a few bronze partitions end-to-end.

Run after ``make up``. Materializes 3 months of TLC + weather against the
running MinIO. Idempotent — re-running is a no-op if data is already landed.
"""

from __future__ import annotations

from dagster import DagsterInstance, materialize

from sentinel.assets.bronze.tlc import bronze_tlc_yellow
from sentinel.assets.bronze.weather import bronze_weather_nyc_daily
from sentinel.resources import ObjectStorage
from sentinel.settings import get_settings

PARTITIONS = ["2024-01-01", "2024-02-01", "2024-03-01"]


def main() -> int:
    s = get_settings()
    storage = ObjectStorage(
        endpoint=s.minio_endpoint,
        access_key=s.minio_access_key,
        secret_key=s.minio_secret_key,
        secure=s.minio_secure,
    )

    instance = DagsterInstance.ephemeral()
    failed: list[str] = []
    for p in PARTITIONS:
        result = materialize(
            [bronze_tlc_yellow, bronze_weather_nyc_daily],
            partition_key=p,
            resources={"storage": storage},
            instance=instance,
            raise_on_error=False,
        )
        status = "ok" if result.success else "fail"
        print(f"[{p}] {status}")
        if not result.success:
            failed.append(p)

    if failed:
        print(f"failed partitions: {failed}")
        return 1
    print("done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
