from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Process-wide config. Loaded once, cached via :func:`get_settings`."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # MinIO / S3-compatible storage. Endpoint is host:port without scheme —
    # that's how minio-py wants it.
    minio_endpoint: str = Field(default="localhost:9000")
    minio_access_key: str = Field(default="minio")
    minio_secret_key: str = Field(default="minio12345")
    minio_secure: bool = Field(default=False)

    bucket_bronze: str = Field(default="sentinel-raw")
    bucket_incidents: str = Field(default="sentinel-incidents")

    # NYC is hard-coded for now. Weather supports a single lat/lon per env;
    # multi-city goes in phase 2 when we actually need it.
    weather_lat: float = Field(default=40.7128)
    weather_lon: float = Field(default=-74.0060)
    weather_tz: str = Field(default="America/New_York")

    log_level: str = Field(default="INFO")
    log_json: bool = Field(default=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
