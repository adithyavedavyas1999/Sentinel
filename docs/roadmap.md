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

### Week 7 — chaos harness + LiteLLM scaffold

**Goals:** Build the failure injection tool. Stand up LiteLLM. No agent
logic yet.

**Deliverables:**

- `scripts/inject_failure.py` with at least 4 failure modes: 5xx from TLC,
  Open-Meteo schema change, DuckDB lock contention, dbt model SQL error.
- `sentinel/agent/llm.py`: thin LiteLLM wrapper, Groq default, env-switchable
  to Anthropic/OpenAI. Mock client for tests.
- A short notebook (yes, a notebook — kept in `docs/notebooks/`) showing
  prompt iteration. Mark it WIP and never refactor it. Real repos have
  one ugly notebook.

**DoD:** Injection script reproducibly breaks 4 failure paths. LLM wrapper
returns structured JSON given a fake log payload.

**Commits:**

- milestone: `chaos: failure injection harness`
- milestone: `agent: litellm wrapper + groq default`
- WIP: `notebook: prompt iteration scratch`

---

### Week 8 — context retrieval (Qdrant)

**Goals:** Build the retrieval layer the agent will use: run history,
lineage, asset docs.

**Deliverables:**

- `sentinel/agent/context.py`: pulls dbt manifest, recent run history from
  Dagster's GraphQL, last N log lines for the failed asset.
- Qdrant container in compose.
- Embedding pipeline: chunk historical incidents + dbt model docs, embed
  with a small model (consider `bge-small` via fastembed to keep it local).

**DoD:** Given a failed asset key, `context.build(asset_key)` returns a
well-formed dict with lineage, logs, and top-k similar past incidents.

**Commits:**

- milestone: `agent: context builder (lineage + logs + qdrant retrieval)`
- WIP: `qdrant: bump client version`
- WIP: `actually persist the qdrant volume`

---

### Week 9 — LangGraph diagnostic agent

**Goals:** First end-to-end agent: failure → context → diagnosis → incident
report. **No remediation yet.**

**Deliverables:**

- `sentinel/agent/graph.py`: LangGraph state machine. Nodes: classify,
  gather context, diagnose, format incident.
- Dagster sensor: on asset failure, fire the agent and write the incident
  to a `incidents/` MinIO bucket as JSON.
- Two unit tests against recorded fixtures from the chaos harness.

**DoD:** Inject a known failure → incident JSON appears in MinIO with a
plausible diagnosis and a proposed-fix string.

**Commits:**

- milestone: `agent: langgraph diagnostic loop (read-only)`
- WIP: `sensor: deduplicate consecutive failures`

---

### Week 10 — remediation allowlist + refactor

**Goals:** Add the constrained remediation actions. **Refactor #2** on the
agent graph — the week-9 shape is probably wrong; expect to split a node.

**Deliverables:**

- Allowlisted actions: retry-with-backoff, partition-window slip, schema-
  drift coerce-to-string (per ADR-004).
- Each action has: a guard, an execute, a rollback, and a fixture-based
  unit test.
- Refactor commit splits the `diagnose` node into `classify` +
  `propose_action` once it's clear they have different prompts.

**DoD:** Inject 3 of the 4 chaos failures → agent remediates and the pipeline
finishes green. The 4th (SQL error) files an incident only.

**Commits:**

- milestone: `agent: allowlisted remediation (retry, slip, coerce)`
- milestone: `refactor: split diagnose into classify+propose`
  - body: "the single 'diagnose' node was carrying two prompts. splitting
    so we can iterate on each independently."
- WIP: `agent: tighten the slip-window guard`

---

### Week 11 — FastAPI + Streamlit incident dashboard

**Goals:** Give the agent's output a face. Read-only UI.

**Deliverables:**

- FastAPI service exposing `/incidents`, `/incidents/{id}`, `/health`.
- Streamlit page that lists open incidents, shows the diagnosis, the proposed
  fix, links to relevant Dagster + Grafana URLs.
- One-click "approve remediation" button. (It writes an approval record;
  the agent re-runs in remediation mode on next sensor tick.)

**DoD:** Demo path: trigger a chaos failure → see the incident in Streamlit
within 60s → approve → pipeline recovers.

**Commits:**

- milestone: `api: fastapi incident endpoints`
- milestone: `ui: streamlit incident dashboard`
- WIP: `streamlit: stop double-fetching on rerun`

---

### Week 12 — polish, blog, demo

**Goals:** Make it presentable without overpolishing. **Refactor #3** if
something has been bugging me — likely the IO manager between Dagster and
DuckDB.

**Deliverables:**

- Rewrite README in your own voice. Strip any remaining AI cadence.
- Architecture doc updated with what actually shipped.
- Short demo video (Loom or similar). 4-6 minutes, not 15.
- Blog post draft for personal site: "What I learned trying to build a
  self-healing pipeline." Be specific about what failed.
- One observable bug left documented in `docs/known-issues.md`. Real
  projects have known issues; pretending you don't have any is the tell.

**DoD:** A stranger can clone, run `make demo`, and see the demo path work.
The blog post is up. The README is yours.

**Commits:**

- milestone: `docs: rewrite readme in human voice`
- milestone: `refactor: simplify duckdb IO manager`
- WIP: `typo`
- WIP: `bump deps before release`

---

## Planned refactors recap

1. **Week 4:** Python silver → dbt silver. Forced by dbt entry.
2. **Week 10:** Single agent `diagnose` node → `classify` + `propose_action`.
3. **Week 12:** Tidy the IO manager. Or something else that's been annoying
   me — leave the slot open.

Visible evolution is the point. Don't hide it.
