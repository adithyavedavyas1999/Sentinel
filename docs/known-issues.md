# Known issues

Real projects have known issues. Pretending you don't have any is the tell.
This file lists ones I'm aware of but didn't fix because the cost is too
high right now or because the fix needs infra I'm not setting up.

## P0 — wrong behavior under specific conditions

None at the time of writing. If something shows up here it should also
have an open incident and a failing test.

## P1 — incomplete coverage

### `sentinel.sensors.failure_capture` is omitted from coverage

The sensor that captures Dagster failures into the incident store is
exercised only by the integration suite (it needs a live Dagster
instance and a failing run). Unit-mocking the failure path is more
effort than it's worth — the assertions would all be on the mocks,
not on the sensor. Long-term fix: stand up a fixture that runs a
single failing asset against `DagsterInstance.ephemeral()` and reads
the resulting incident row. Tracked.

### Streamlit dashboard is render-only-untested

`sentinel/ui/dashboard.py` is excluded from coverage. It's pure
rendering + HTTP-glue and the API endpoints it consumes are fully
covered. A real e2e test would need a headless browser; not worth it
for a portfolio repo.

## P2 — code smell I'm aware of

### Chaos module's own MinIO helpers

`sentinel/chaos/__init__.py` reaches into MinIO via its own
`_storage()` / `_read_parquet` / `_write_parquet` helpers instead of
going through the `ObjectStorage` resource. This was the original
plan for a Week 12 refactor; on reflection the right move is the
opposite (introduce a thin "bytes in / bytes out" protocol so a
future IO-manager-proper rewrite doesn't touch chaos). For now the
duplication stands, because consolidating it before the IO manager is
re-designed would be wasted work.

### `sqlglot[rs]` deprecation warning

Every test run emits a deprecation warning from a transitive sqlglot
dependency (dbt-duckdb pulls it). It's a no-op for us until they
ship a breaking version. If/when that lands, the fix is `uv pip
install sqlglot[c]` (their new fast-parser extra). Not pinning early
because the new extra is itself young.

### Long stack traces still go through the embedding model

`sentinel.agent.embeddings.summarize_for_embedding` already strips
the tail of multiline tracebacks, but it doesn't dedupe common Python
frames. On real production stacks (dozens of identical
``in <module>`` lines) this leaks tokens. The bge-small model
tolerates it but the cosine scores degrade. A regex pass on common
frames is a 30-minute fix; I'll do it when the qdrant search results
start being noticeably worse.

## P3 — paper-cut things I'd fix in a real repo

- `ObjectStorage._client()` rebuilds a `Minio` client on every call.
  Fine for low-volume operations, wasteful for the chaos/coerce path
  which can be called per-partition. Comment is on line 21 of
  `sentinel/resources/storage.py`. Trivial to fix; postponed because
  it requires touching the resource interface and that ripples to
  tests.
- The agent's diagnostic prompt is good enough but not great. It was
  iterated three times in `docs/notebooks/prompt_iteration.ipynb`
  against an LLM with rate limits; a longer iteration on a different
  provider would tighten the rubric (especially for
  `coerce-to-string`, which under-fires).
- `dbt parse` runs eagerly at Dagster import time
  (`prepare_if_dev` in `sentinel/resources/dbt.py`). CI works around
  this with an explicit `dbt parse` step. A cleaner pattern is to
  ship a pre-built manifest as a build artifact; that's a real
  project-level decision, deliberately not made here.
