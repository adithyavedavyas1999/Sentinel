from __future__ import annotations

from collections.abc import Iterator
from io import BytesIO

from dagster import ConfigurableResource
from minio import Minio
from minio.error import S3Error


class ObjectStorage(ConfigurableResource):
    """S3-compatible object storage. Backed by MinIO locally; same API works
    against real S3 with ``minio_secure=True`` and the right endpoint.
    """

    endpoint: str
    access_key: str
    secret_key: str
    secure: bool = False

    # TODO: cache the Minio client across calls. minio-py is thread-safe and
    # holds a urllib3 PoolManager; rebuilding it on every op is wasteful when
    # we get to high-volume materializations. Fine for now.
    def _client(self) -> Minio:
        return Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

    def ensure_bucket(self, bucket: str) -> None:
        client = self._client()
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)

    def object_exists(self, bucket: str, key: str) -> bool:
        client = self._client()
        try:
            client.stat_object(bucket, key)
        except S3Error as exc:
            if exc.code in ("NoSuchKey", "NoSuchObject", "NoSuchBucket"):
                return False
            raise
        return True

    def put_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> int:
        client = self._client()
        self.ensure_bucket(bucket)
        client.put_object(
            bucket,
            key,
            BytesIO(data),
            length=len(data),
            content_type=content_type,
        )
        return len(data)

    def get_bytes(self, bucket: str, key: str) -> bytes:
        """Pull an object into memory.

        Use sparingly — every byte goes through python. For real-volume reads
        from silver/gold layers we use polars + the duckdb httpfs path, which
        bypasses this entirely.
        """
        client = self._client()
        resp = client.get_object(bucket, key)
        try:
            return resp.read()
        finally:
            resp.close()
            resp.release_conn()

    def list_keys(self, bucket: str, prefix: str = "") -> Iterator[str]:
        client = self._client()
        for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
            if obj.object_name is not None:
                yield obj.object_name

    def delete_object(self, bucket: str, key: str) -> None:
        """Remove a single object. Used by remediation rollback paths.

        Swallows ``NoSuchKey`` -- delete-of-missing is idempotent for our
        purposes. Anything else raises so callers can surface the failure.
        """
        client = self._client()
        try:
            client.remove_object(bucket, key)
        except S3Error as exc:
            if exc.code in ("NoSuchKey", "NoSuchObject"):
                return
            raise
