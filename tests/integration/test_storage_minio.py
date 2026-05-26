"""Integration tests against a real MinIO container.

Run with: ``pytest -m integration`` (skipped by default to keep CI fast).
"""
from __future__ import annotations

import socket
import time
from contextlib import closing

import pytest

pytestmark = pytest.mark.integration

try:
    from testcontainers.core.container import DockerContainer  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover
    pytest.skip("testcontainers not installed", allow_module_level=True)

# E402: import is below the testcontainers guard above, intentionally.
from sentinel.resources.storage import ObjectStorage  # noqa: E402


def _wait_port(host: str, port: int, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.settimeout(1.0)
            if s.connect_ex((host, port)) == 0:
                return
        time.sleep(0.5)
    raise TimeoutError(f"{host}:{port} not reachable in {timeout}s")


@pytest.fixture(scope="module")
def minio_container():
    c = (
        DockerContainer("minio/minio:RELEASE.2024-10-13T13-34-11Z")
        .with_env("MINIO_ROOT_USER", "test")
        .with_env("MINIO_ROOT_PASSWORD", "testtest")
        .with_command("server /data")
        .with_exposed_ports(9000)
    )
    c.start()
    try:
        host = c.get_container_host_ip()
        port = int(c.get_exposed_port(9000))
        _wait_port(host, port)
        yield host, port
    finally:
        c.stop()


def test_put_and_stat_against_real_minio(minio_container):
    host, port = minio_container
    storage = ObjectStorage(
        endpoint=f"{host}:{port}",
        access_key="test",
        secret_key="testtest",
        secure=False,
    )
    storage.put_bytes("smoke", "hello.txt", b"hi", content_type="text/plain")
    assert storage.object_exists("smoke", "hello.txt")
    assert not storage.object_exists("smoke", "missing.txt")
