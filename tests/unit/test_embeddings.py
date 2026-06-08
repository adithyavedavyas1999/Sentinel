from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from sentinel.agent import embeddings


class _StubBackend:
    def __init__(self, vectors: list[list[float]]) -> None:
        self.vectors = vectors

    def embed(self, texts):
        yield from self.vectors[: len(list(texts))]


def test_embedder_single_string():
    e = embeddings.Embedder(backend=_StubBackend([[1.0, 2.0, 3.0]]))
    out = e.embed("hello")
    assert out == [[1.0, 2.0, 3.0]]
    assert e.dim == 3


def test_embedder_list():
    e = embeddings.Embedder(backend=_StubBackend([[1, 2], [3, 4], [5, 6]]))
    out = e.embed(["a", "b", "c"])
    assert out == [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]


def test_embedder_empty_input():
    e = embeddings.Embedder(backend=_StubBackend([]))
    assert e.embed([]) == []


def test_embedder_dim_default_when_unused():
    e = embeddings.Embedder(backend=_StubBackend([]))
    assert e.dim == embeddings.DEFAULT_DIM


def test_summarize_for_embedding_drops_empty():
    text = embeddings.summarize_for_embedding(
        {
            "asset_key": "bronze.tlc_yellow",
            "error_type": "HTTPStatusError",
            "error_message": "503",
        }
    )
    assert "bronze.tlc_yellow" in text
    assert "HTTPStatusError" in text
    assert "503" in text


def test_summarize_includes_proposed_fix():
    text = embeddings.summarize_for_embedding(
        {
            "asset_key": "x",
            "error_type": "y",
            "error_message": "z",
            "proposed_fix": "retry-with-backoff",
        }
    )
    assert "retry-with-backoff" in text


def test_qdrant_point_id_passes_uuid():
    uuid = "12345678-1234-1234-1234-123456789012"
    assert embeddings._qdrant_point_id(uuid) == uuid


def test_qdrant_point_id_hashes_non_uuid():
    pid = embeddings._qdrant_point_id("not-a-uuid")
    assert isinstance(pid, int)
    assert pid >= 0


def _fake_qdrant_with_collections(names: list[str]) -> Any:
    c = MagicMock()
    c.get_collections.return_value.collections = [MagicMock(name=n) for n in names]
    # MagicMock(name=...) sets the mock's __repr__, not the .name attribute.
    # Patch that explicitly.
    for col, n in zip(c.get_collections.return_value.collections, names, strict=True):
        col.name = n
    return c


def test_ensure_collection_creates_when_missing():
    fake = _fake_qdrant_with_collections([])
    idx = embeddings.IncidentIndex(
        client=fake,
        embedder=embeddings.Embedder(backend=_StubBackend([])),
    )
    idx.ensure_collection()
    fake.create_collection.assert_called_once()


def test_ensure_collection_skips_when_present():
    fake = _fake_qdrant_with_collections(["incidents"])
    idx = embeddings.IncidentIndex(
        client=fake,
        embedder=embeddings.Embedder(backend=_StubBackend([])),
    )
    idx.ensure_collection()
    fake.create_collection.assert_not_called()


def test_upsert_calls_client():
    fake = _fake_qdrant_with_collections(["incidents"])
    idx = embeddings.IncidentIndex(
        client=fake,
        embedder=embeddings.Embedder(backend=_StubBackend([[0.1] * 4])),
    )
    idx.upsert("inc-1", "boom", {"asset_key": "x"})
    fake.upsert.assert_called_once()
    kwargs = fake.upsert.call_args.kwargs
    assert kwargs["collection_name"] == "incidents"
    [point] = kwargs["points"]
    assert point.vector == [0.1, 0.1, 0.1, 0.1]


def test_search_returns_typed_hits():
    fake = _fake_qdrant_with_collections(["incidents"])
    hit = MagicMock()
    hit.id = "inc-9"
    hit.score = 0.92
    hit.payload = {
        "incident_id": "inc-9",
        "asset_key": "bronze.tlc_yellow",
        "error_type": "ChaosTriggered",
    }
    fake.search.return_value = [hit]

    idx = embeddings.IncidentIndex(
        client=fake,
        embedder=embeddings.Embedder(backend=_StubBackend([[0.0]])),
    )
    [r] = idx.search("upstream 5xx", top_k=3, asset_key="bronze.tlc_yellow")
    assert r.incident_id == "inc-9"
    assert r.score == pytest.approx(0.92)
    assert r.asset_key == "bronze.tlc_yellow"


def test_search_with_no_payload_is_safe():
    fake = _fake_qdrant_with_collections(["incidents"])
    hit = MagicMock()
    hit.id = "x"
    hit.score = 0.1
    hit.payload = None
    fake.search.return_value = [hit]

    idx = embeddings.IncidentIndex(
        client=fake,
        embedder=embeddings.Embedder(backend=_StubBackend([[0.0]])),
    )
    [r] = idx.search("anything")
    assert r.asset_key == ""
    assert r.error_type == ""
