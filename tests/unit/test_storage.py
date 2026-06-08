from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from minio.error import S3Error

from sentinel.resources.storage import ObjectStorage


def _resource() -> ObjectStorage:
    return ObjectStorage(
        endpoint="minio:9000",
        access_key="k",
        secret_key="s",
        secure=False,
    )


def _s3_error(code: str) -> S3Error:
    return S3Error(
        code=code,
        message="m",
        resource="/r",
        request_id="rid",
        host_id="hid",
        response=None,
    )


@pytest.fixture
def mock_minio(mocker):
    client = MagicMock()
    client.bucket_exists.return_value = True
    mocker.patch("sentinel.resources.storage.Minio", return_value=client)
    return client


def test_put_bytes_writes_to_client(mock_minio):
    r = _resource()
    n = r.put_bytes("bucket", "key", b"hello", content_type="text/plain")

    assert n == 5
    mock_minio.put_object.assert_called_once()
    args, kwargs = mock_minio.put_object.call_args
    assert args[0] == "bucket"
    assert args[1] == "key"
    assert kwargs["length"] == 5
    assert kwargs["content_type"] == "text/plain"


def test_object_exists_true_when_stat_succeeds(mock_minio):
    assert _resource().object_exists("b", "k") is True


def test_object_exists_false_on_no_such_key(mock_minio):
    mock_minio.stat_object.side_effect = _s3_error("NoSuchKey")
    assert _resource().object_exists("b", "k") is False


def test_object_exists_propagates_unexpected_errors(mock_minio):
    mock_minio.stat_object.side_effect = _s3_error("AccessDenied")
    with pytest.raises(S3Error) as exc:
        _resource().object_exists("b", "k")
    assert exc.value.code == "AccessDenied"


def test_ensure_bucket_creates_when_missing(mock_minio):
    mock_minio.bucket_exists.return_value = False
    _resource().ensure_bucket("new-bucket")
    mock_minio.make_bucket.assert_called_once_with("new-bucket")


def test_get_bytes_reads_full_object_and_releases(mock_minio):
    resp = MagicMock()
    resp.read.return_value = b"hello"
    mock_minio.get_object.return_value = resp

    out = _resource().get_bytes("b", "k")

    assert out == b"hello"
    mock_minio.get_object.assert_called_once_with("b", "k")
    resp.close.assert_called_once()
    resp.release_conn.assert_called_once()
