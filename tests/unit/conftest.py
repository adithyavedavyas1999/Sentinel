from __future__ import annotations

from collections.abc import Iterator

import pytest

from sentinel.resources.storage import ObjectStorage

# dagster model_copies resources when binding them to assets, so a per-instance
# store doesn't work. Use a process-wide store keyed by bucket and clear it
# between tests via the autouse fixture below.
_STORE: dict[str, dict[str, bytes]] = {}


class FakeStorage(ObjectStorage):
    """In-memory ObjectStorage. Same surface, no MinIO required."""

    def ensure_bucket(self, bucket: str) -> None:
        _STORE.setdefault(bucket, {})

    def object_exists(self, bucket: str, key: str) -> bool:
        return key in _STORE.get(bucket, {})

    def put_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> int:
        self.ensure_bucket(bucket)
        _STORE[bucket][key] = data
        return len(data)

    def get_bytes(self, bucket: str, key: str) -> bytes:
        return _STORE[bucket][key]

    def list_keys(self, bucket: str, prefix: str = "") -> Iterator[str]:
        for k in _STORE.get(bucket, {}):
            if k.startswith(prefix):
                yield k


@pytest.fixture
def fake_storage() -> FakeStorage:
    return FakeStorage(
        endpoint="fake:9000",
        access_key="fake",
        secret_key="fake",
        secure=False,
    )


@pytest.fixture(autouse=True)
def _instant_tenacity_retries(monkeypatch):
    """Skip the exponential backoff sleep in unit tests so retry coverage
    doesn't add seconds to every run. Real waits still apply in integration.
    """
    monkeypatch.setattr("tenacity.nap.time.sleep", lambda *_a, **_k: None)


@pytest.fixture(autouse=True)
def _reset_fake_store():
    _STORE.clear()
    yield
    _STORE.clear()
