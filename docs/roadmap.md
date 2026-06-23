# Roadmap

12 weeks. Phase 1 ends after week 6. Phase 2 picks up the agent.

Each week below lists goals, deliverables, and a definition of done. Some
commits are marked **WIP** — those should land as small, possibly-typoed
commits ("fix import", "use the config value this time"). **Milestone**
commits are the ones a reviewer would land on if they `git log --oneline`.

Plan to deliberately revise a few choices over time. Three planned refactors
are flagged below — these go in with commit messages that admit the change.

---

## Phase 1 — pipeline, dbt, observability

### Week 1 — skeleton + bronze ingest [DONE]

**Goals:** repo bootstrap, docker-compose up cleanly, Dagster running, bronze
ingest for TLC and weather landing in MinIO.

**Note:** original roadmap had bronze TLC in week 2 and weather in week 3.
Pulled both forward into week 1 since the ingest patterns are nearly identical
and splitting them across two weeks felt artificial. Silver + asset checks
still land in week 3.

**Deliverables:**

- Repo skeleton + docker-compose stack.
- `sentinel/` package with split between pure-python `ingest/` and Dagster
  `assets/` — keeps the fetchers testable without Dagster.
- `ObjectStorage` ConfigurableResource over minio-py with idempotent
  `object_exists` / `put_bytes`.
- Monthly-partitioned bronze assets: `bronze.tlc_yellow`, `bronze.weather_nyc_daily`.
- Tenacity-based retry on 5xx/429/network errors; 4xx fails immediately.
- structlog JSON logging, env-driven config via pydantic-settings.
- pytest suite: unit tests for fetchers (respx) + storage (mock minio) +
  asset wiring (build_asset_context). One integration test against
  testcontainers MinIO, gated behind `-m integration`.
- `scripts/demo.py` materializes 3 months of bronze end-to-end.

**DoD:** `make setup && make up && make demo` lands six parquet files
(3 TLC, 3 weather) in MinIO. Re-running is a no-op.

**Commits:**

- milestone: `scaffold: dagster, settings, structlog, minio resource`
- milestone: `bronze: tlc parquet ingest with tenacity retry`
- milestone: `bronze: open-meteo daily weather, polars->parquet`
- milestone: `tests: respx-mocked fetchers + fake storage`
- WIP: `tlc: tighten retryable predicate (no 4xx retries)`
- WIP: `oops missed an import`
- WIP: `bump testcontainers in dev deps`

---

### Week 2 — interim python silver [DONE]

Interim python silver landed. Polars-based join in
`sentinel/assets/silver/trips_weather.py`. Two asset checks
(`rowcount_positive`, `pickup_ts_not_null`). Marked DEPRECATED in the file
header; kept around until dbt silver runs cleanly for two consecutive
partitions, then will be deleted in a follow-up commit.

---

### Week 3 — bronze tightening [DONE]

Bronze assets now emit row count, schema fingerprint, and pickup_ts bounds
in their metadata. `tlc_freshness_sensor` HEADs the next month's URL and
fires a partitioned run when upstream publishes. Sensor is shipped
STOPPED by default — opt-in from the Dagster UI when you want it polling.

---

### Week 4 — dbt comes in; rewrite silver [DONE — refactor #1]

dbt-core + dbt-duckdb landed. Staging models `stg_tlc_yellow`,
`stg_weather_nyc_daily`. Silver moved to dbt at
`dbt/models/marts/silver_trips_weather.sql`. `dagster-dbt` integration
surfaces dbt models as Dagster assets via `sentinel_dbt_assets`.

The python silver still exists in the working tree, marked DEPRECATED.
Open follow-up: `refactor: delete week-2 python silver` once we have two
clean dbt silver runs in a row.

---

### Week 5 — gold marts + dbt tests [DONE]

Gold: `fct_trips_daily` (daily-by-zone), `dim_zones` (seeded). dbt tests
cover unique, not_null, accepted_values, relationships,
dbt_utils.unique_combination_of_columns, and two
dbt_expectations checks (fare-amount bounds, trip-count non-negative).
Two custom generic tests in `dbt/tests/generic/`:
`sentinel_no_future_dates` and `sentinel_row_count_within_tolerance`.

Data model documented in [docs/data-model.md](data-model.md).

---

### Week 6 — observability + CI + incidents + chaos + polish [DONE]

`sentinel/observability/metrics.py` exposes Prometheus counters and a
histogram on :9464. Bronze assets are instrumented for materialization
counts, ingest latency, and rows landed. Loki + Promtail added to the
compose stack. Two Grafana dashboards provisioned:
`docker/grafana/dashboards/pipeline_health.json`,
`data_quality.json`.

GitHub Actions CI runs ruff, mypy, pytest (>75% gate), and dbt parse/compile.

**Failure capture:** `failure_capture_sensor` writes structured incidents
into a sqlite store (`sentinel/quality/incidents.py`). The Phase 2 agent
reads from here. CLI: `sentinel incident list|show|resolve`. Chosen sqlite
over Postgres for now — see ADR notes in the incidents module header.

**Chaos harness:** `sentinel chaos inject <scenario>` with 8 scenarios
scaffolded. Two have working implementations (`tlc_schema_drift`,
`null_spike`); the rest are stubs to be fleshed out in Phase 2 alongside
the agent eval suite.

**Phase 1 demo checkpoint:** complete. See [demo-script.md](demo-script.md).

---

## Phase 1 retrospective (write this yourself after recording the demo)

The roadmap had three planned refactors. One landed (Python silver →
dbt silver, Week 4). The other two are still planned (agent node split,
Week 10; IO manager cleanup, Week 12). Worth checking the roadmap's
predictions against reality once Phase 2 starts.

---

## Phase 2 — self-healing

### Week 7 — chaos harness + LiteLLM scaffold [DONE]

Nine chaos scenarios are wired end-to-end. Originally shipped six and
left `volume_drop` / `late_partition` as stubs pending "future infra";
that turned out to be unnecessary — the injection itself doesn't need
historical baselines or partition-window sensors, those live in the
agent (week 10), not here. Filled both in same week.

Two flavors:

- **State-flag** (`tlc_5xx`, `weather_429`, `duckdb_lock`,
  `late_partition`): a JSON marker in `data/chaos/state.json`; bronze +
  dbt assets check on materialize and raise `ChaosTriggered`. Reversed
  by `sentinel chaos clear <name>`.
- **Destructive** (`tlc_schema_drift`, `weather_schema_change`,
  `null_spike`, `dbt_sql_error`, `volume_drop`): mutate parquet/SQL on
  disk. `dbt_sql_error` has a clean reverse; the others require
  re-running ingest.

`sentinel/agent/llm.py` ships a LiteLLM wrapper (Groq default,
env-switchable) with retry on rate-limit/5xx, a JSON-mode helper that
validates against a pydantic schema, and a `MockLLMClient` for tests.

Notebook lives at `docs/notebooks/prompt_iteration.ipynb`. WIP and ugly
on purpose. Three prompt versions, two of which didn't work; leaving them
so we don't re-derive them.

**Deferred:** `scripts/inject_failure.py` — ended up not being needed
because the same surface lives behind `sentinel chaos inject` and gets
exercised in CI via the chaos tests.

---

### Week 8 — context retrieval (Qdrant) [DONE]

`sentinel/agent/context.py` builds an `IncidentContext` from up to five
sources: the incident row, dbt lineage from `target/manifest.json`, recent
runs via `DagsterInstance.get_run_records()` (chosen over GraphQL so the
agent works from the daemon process, not the webserver), recent log lines,
and top-k similar past incidents from Qdrant. Each source is optional —
if dbt manifest isn't available or Qdrant is down, that field is empty
and the rest still works.

`sentinel/agent/embeddings.py`: fastembed (BAAI/bge-small-en-v1.5, 384-dim,
ONNX, no torch dep) for vectors; `IncidentIndex` wraps `QdrantClient` for
upsert + cosine search with optional asset_key filter. Long stack traces
poison the small-model embedding so we keep them in the payload but not
in the embedded text — see `summarize_for_embedding`.

Qdrant added to compose at `:6333`. `scripts/index_incidents.py` does a
one-shot backfill from sqlite into Qdrant; rerun after a rebuild or model
change.

**Open follow-up:** pgvector vs. Qdrant — Postgres is already in the stack
and a single fewer container would be nice. Punted to keep filtering
ergonomics for now; revisit if Qdrant becomes the only reason to run a
new container per environment.

---

### Week 9 — LangGraph diagnostic agent [DONE]

`sentinel/agent/graph.py` is a LangGraph state machine with four nodes
(classify → gather_context → diagnose → format_incident). Only the
diagnose node calls the LLM; classification is heuristic and lives in
``_classify`` so we can test the failure-handling without burning
tokens.

The agent surface stays decoupled from Dagster: ``AgentDeps`` carries
``LLMClient`` + ``IncidentStore`` + optional ``DagsterInstance`` +
optional ``IncidentIndex``, so tests construct a graph with mocks and
the sensor wires the real things. Output is a ``incident_report`` dict
written verbatim to ``sentinel-incidents`` MinIO.

Sensor is `diagnostic_agent_sensor` — read-only, default-stopped (LLM
calls cost money). Re-tick is a no-op for already-diagnosed incidents.

---

### Week 10 — remediation allowlist + refactor [DONE — refactor #2]

`sentinel/agent/remediation/` ships three actions (`retry-with-backoff`,
`partition-window-slip`, `coerce-to-string`) behind a uniform protocol:
``guard``, ``execute``, ``rollback``. Each one is a pure function of
``(incident, RemediationDeps)``, fixture-tested without a Dagster
instance.

Refactor that landed this week: split the agent graph's ``diagnose``
node into ``diagnose`` (the LLM call) + ``propose_action`` (translate
``proposed_fix`` into a concrete remediation plan, validated against
the same allowlist). Reasons it was worth splitting:

- The two have different inputs (LLM-shaped vs. registry-shaped).
- The dashboard's approve button reuses ``propose_action`` cleanly.
- Off-allowlist proposals are now stamped with a structured
  ``proposed_action.status='skipped'`` reason instead of being
  invisibly dropped.

The agent itself never executes an action whose name is off the
allowlist — ``validate_remediation_claim`` plus the registry-only
``dispatch`` belt-and-braces this. Same logic gates the FastAPI
approve endpoint, so the dashboard cannot apply an action the agent
itself isn't allowed to apply.

---

### Week 11 — FastAPI + Streamlit incident dashboard [DONE]

`sentinel/api/main.py`:

- `GET /health` — readiness probe.
- `GET /incidents` / `GET /incidents/{id}` — list open + show detail,
  rehydrating the stored incident report from MinIO.
- `POST /incidents/{id}/resolve` — manual close.
- `POST /incidents/{id}/approve` — dispatches the same allowlisted
  remediation that the agent would run.
- `GET /allowlist` — discovery endpoint the UI uses to populate the
  approve dropdown.

`sentinel/ui/dashboard.py`: single-page Streamlit (two columns: list +
detail). Lets a reviewer click through diagnoses and approve fixes
without leaving the page.

Docker Compose ships `sentinel-api` (uvicorn :8000) and
`sentinel-dashboard` (streamlit :8501) on the same image we use for
Dagster — one Python install for everything.

**Deferred:** "deep link to Dagster + Grafana" from the dashboard. The
URLs are environment-specific and stale links are worse than no links.
Phase 3 candidate.

---

### Week 12 — polish, IO manager refactor, known-issues doc [DONE]

This week was mostly text + cleanup:

- README rewritten to reflect the shape that actually shipped (12
  weeks vs. the original 6-then-extend pitch).
- `docs/known-issues.md` lists three real ones — coverage gap on
  ``sentinel.sensors.failure_capture``, a sqlglot deprecation warning
  that's not ours to fix, and the IO-manager TODO below.
- **Refactor #3** ended up *not* being a heavy IO-manager rewrite.
  The existing storage resource is fine; the actual annoyance was that
  the chaos module talked to MinIO via its own ad-hoc helpers
  (`_storage()`, `_read_parquet`, `_write_parquet`), duplicating the
  resource. Folded those into a thin internal protocol so a future
  IO-manager-proper refactor doesn't have to touch chaos. Tracked as
  open in known-issues.

**Deferred:** demo video + blog post — those are human deliverables
that don't live in the repo. Hooks are in `docs/demo-script.md` and
`docs/blog/phase-1-outline.md` for whoever records them.

---

## Planned refactors recap

1. **Week 4:** Python silver → dbt silver. Forced by dbt entry.
2. **Week 10:** Single agent `diagnose` node → `classify` + `propose_action`.
3. **Week 12:** Tidy the IO manager. Or something else that's been annoying
   me — leave the slot open.

Visible evolution is the point. Don't hide it.
