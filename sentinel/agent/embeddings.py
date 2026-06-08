"""Embeddings + Qdrant retrieval for similar past incidents.

Decisions worth knowing about:

- **fastembed over sentence-transformers**: fastembed ships ONNX weights
  and runs without a torch dependency. That keeps the docker image
  smaller and the cold-start fast. The default model
  ``BAAI/bge-small-en-v1.5`` is 384-dim, ~80MB on disk, decent for English
  technical text. We can revisit if recall starts failing the eval harness.

- **Qdrant over pgvector / chroma**: the pipeline already runs Postgres for
  Dagster. Reusing it for vectors via pgvector would have been tidier, but
  Qdrant's filtering API is meaningfully better for the shape of queries
  we want (asset_key + error_type + recency). Chroma was the third option;
  rejected for being a bit too in-flux on the operational side.

- **One collection per environment, not per asset**: tried per-asset and
  it made retrieval awkward when the agent wants "anything similar across
  the pipeline." Filtering by asset_key inside one collection is plenty.

The :class:`IncidentIndex` is the only thing the rest of the agent talks
to. :class:`Embedder` is exposed for testing and for the backfill script.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from sentinel.observability.logging import get_logger

log = get_logger(__name__)

DEFAULT_MODEL = "BAAI/bge-small-en-v1.5"
DEFAULT_DIM = 384
DEFAULT_COLLECTION = "incidents"


class _EmbeddingBackend(Protocol):
    def embed(self, texts: Sequence[str]) -> Iterable[list[float]]: ...


class Embedder:
    """Tiny shim over fastembed's TextEmbedding.

    Reasoning for the shim: fastembed's ``embed`` returns a generator of
    numpy arrays. Materializing into a list of plain floats is what
    Qdrant's client wants and what tests want to assert against. We do
    both conversions here so callers don't have to.
    """

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        *,
        backend: _EmbeddingBackend | None = None,
    ) -> None:
        self.model = model
        self._backend = backend
        self._dim: int | None = None

    def _ensure(self) -> _EmbeddingBackend:
        if self._backend is not None:
            return self._backend
        from fastembed import TextEmbedding

        self._backend = TextEmbedding(model_name=self.model)
        return self._backend

    def embed(self, texts: str | Sequence[str]) -> list[list[float]]:
        items: Sequence[str] = [texts] if isinstance(texts, str) else texts
        if not items:
            return []
        out: list[list[float]] = []
        for vec in self._ensure().embed(list(items)):
            # numpy -> python list. fastembed yields np.float32 vectors.
            as_list = [float(x) for x in vec]
            out.append(as_list)
            if self._dim is None:
                self._dim = len(as_list)
        return out

    @property
    def dim(self) -> int:
        return self._dim if self._dim is not None else DEFAULT_DIM


@dataclass
class SimilarIncident:
    incident_id: str
    score: float
    asset_key: str
    error_type: str
    payload: dict[str, Any]


class IncidentIndex:
    """Qdrant-backed nearest-neighbor over incident text.

    The "text" we embed is a short summary built from the incident's
    error_type + error_message + asset_key. Dropping in stack traces tanked
    retrieval quality early on (long traces dominate the embedding); we
    keep them in the payload so the agent can read them post-retrieval but
    don't include them in the embedded text.
    """

    def __init__(
        self,
        *,
        url: str = "http://localhost:6333",
        api_key: str | None = None,
        collection: str = DEFAULT_COLLECTION,
        embedder: Embedder | None = None,
        client: Any | None = None,
        dim: int = DEFAULT_DIM,
    ) -> None:
        self.url = url
        self.api_key = api_key
        self.collection = collection
        self.embedder = embedder or Embedder()
        self._client = client
        self._dim = dim

    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        from qdrant_client import QdrantClient

        self._client = QdrantClient(url=self.url, api_key=self.api_key)
        return self._client

    def ensure_collection(self) -> None:
        from qdrant_client.http import models as qm

        c = self._ensure_client()
        existing = {col.name for col in c.get_collections().collections}
        if self.collection in existing:
            return
        c.create_collection(
            collection_name=self.collection,
            vectors_config=qm.VectorParams(size=self._dim, distance=qm.Distance.COSINE),
        )
        log.info("incident_index.created_collection", name=self.collection, dim=self._dim)

    def upsert(self, incident_id: str, text: str, payload: dict[str, Any]) -> None:
        from qdrant_client.http import models as qm

        c = self._ensure_client()
        [vector] = self.embedder.embed(text)
        c.upsert(
            collection_name=self.collection,
            points=[
                qm.PointStruct(id=_qdrant_point_id(incident_id), vector=vector, payload=payload),
            ],
        )

    def search(
        self,
        text: str,
        *,
        top_k: int = 5,
        asset_key: str | None = None,
    ) -> list[SimilarIncident]:
        from qdrant_client.http import models as qm

        c = self._ensure_client()
        [vector] = self.embedder.embed(text)
        flt: qm.Filter | None = None
        if asset_key is not None:
            flt = qm.Filter(
                must=[qm.FieldCondition(key="asset_key", match=qm.MatchValue(value=asset_key))]
            )
        # search() is deprecated in newer qdrant-client in favor of query_points, but
        # search() is still supported and easier to mock; revisit when we bump.
        hits = c.search(
            collection_name=self.collection,
            query_vector=vector,
            limit=top_k,
            query_filter=flt,
        )
        return [
            SimilarIncident(
                incident_id=str(h.payload.get("incident_id") if h.payload else h.id),
                score=float(h.score),
                asset_key=str(h.payload.get("asset_key", "")) if h.payload else "",
                error_type=str(h.payload.get("error_type", "")) if h.payload else "",
                payload=dict(h.payload or {}),
            )
            for h in hits
        ]


def summarize_for_embedding(payload: dict[str, Any]) -> str:
    """Build the text we actually embed.

    Kept short on purpose. Long stack traces poison the small-model
    embedding; keep those in the structured payload for the agent to read
    after retrieval.
    """
    bits = [
        payload.get("asset_key", ""),
        payload.get("error_type", ""),
        payload.get("error_message", ""),
    ]
    pf = payload.get("proposed_fix")
    if pf:
        bits.append(f"prior fix: {pf}")
    return "\n".join(b for b in bits if b)


def _qdrant_point_id(incident_id: str) -> int | str:
    """Qdrant accepts either int or string-uuid as point id.

    We pass through anything that's already valid uuid (str), else fall
    back to a stable hash int. Most incidents are uuids so this is a
    no-op in practice.
    """
    if _looks_like_uuid(incident_id):
        return incident_id
    # 64-bit truncation of python's hash; fine for collision-resistance at
    # the volumes we'll see here.
    return abs(hash(incident_id)) & 0xFFFFFFFFFFFFFFFF


def _looks_like_uuid(s: str) -> bool:
    if len(s) != 36:
        return False
    parts = s.split("-")
    return len(parts) == 5 and all(_is_hex(p) for p in parts)


def _is_hex(s: str) -> bool:
    try:
        int(s, 16)
        return True
    except ValueError:
        return False
