# Sentinel 🛡

[![ci](https://github.com/adithyavedavyas1999/Sentinel/actions/workflows/ci.yml/badge.svg)](https://github.com/adithyavedavyas1999/Sentinel/actions/workflows/ci.yml)
![python](https://img.shields.io/badge/python-3.11+-blue.svg)
![license](https://img.shields.io/badge/license-MIT-green.svg)

A local data pipeline that tries to diagnose itself when it breaks.

Sentinel ingests NYC TLC trip data and Open-Meteo weather, lands it through a
bronze/silver/gold warehouse in DuckDB, and runs dbt models on top. When an
asset fails or a data-quality check trips, an LLM agent reads the failed asset's
logs, the dbt manifest, and recent run history, then either applies a narrow
auto-remediation or files a structured incident with a proposed fix. The agent
does not get to mutate warehouse state outside a small allowlist.

The point of the project is not the data. The point is the failure-handling
loop: structured logs in, lineage and metadata in, decision out, audit trail
preserved.

## Status

All twelve weeks of the roadmap are landed. End-to-end flow: chaos
injection → asset failure → incident captured to sqlite → agent
diagnoses via LLM → optional auto-remediation against an allowlist →
incident JSON written to MinIO → FastAPI + Streamlit dashboard for
human review and approval.

See [docs/roadmap.md](docs/roadmap.md) for what each week shipped,
[docs/known-issues.md](docs/known-issues.md) for what I'd fix in a
real codebase, and [docs/architecture.md](docs/architecture.md) for the
ADRs.

Screenshots: still TODO. Easier once the demo is recorded.

## What's inside

- Dagster (assets + sensors) for orchestration
- dbt-core on DuckDB for transforms
- MinIO for raw landing, Postgres for Dagster metadata
- Prometheus + Grafana + Loki for metrics and logs
- LiteLLM (Groq default) for the diagnosis prompt; provider-swappable
  via env var
- fastembed + Qdrant for similar-incident retrieval
- LangGraph for the agent state machine — orchestration only, no
  LangChain provider abstractions (see ADR-005)
- FastAPI + Streamlit for the incident dashboard

## Quickstart

```bash
make setup        # uv venv + install
cp .env.example .env
make up           # docker compose up
make seed-data    # pulls a TLC sample
make demo         # materializes the pipeline end-to-end
```

Dagster UI: <http://localhost:3000>
Grafana: <http://localhost:3001>
MinIO console: <http://localhost:9001>
Incident API: <http://localhost:8000/docs> (Swagger)
Incident dashboard: <http://localhost:8501>

Local-only entry points if you don't want the full compose stack:

```bash
make api          # uvicorn on :8000
make dashboard    # streamlit on :8501
```

## Demo path

1. `make demo` — three months of bronze + dbt build.
2. `sentinel chaos inject tlc_5xx` — flips a flag so the next TLC
   materialization will raise.
3. Re-run that partition in Dagster. The failure capture sensor stores
   the incident in sqlite.
4. Enable `diagnostic_agent_sensor` from the Dagster UI. Within ~30s
   the agent diagnoses, proposes `retry-with-backoff`, clears the
   chaos flag, and the incident report appears in
   `sentinel-incidents/incidents/<id>.json`.
5. Open the Streamlit dashboard, hit "Apply" on retry-with-backoff
   (idempotent — same action the agent ran). Pipeline resumes.

See [docs/demo-script.md](docs/demo-script.md) for the long version.

## Docs

- [Architecture and ADRs](docs/architecture.md)
- [Roadmap](docs/roadmap.md)
- [Data model](docs/data-model.md)
- [Chaos scenarios](docs/chaos-scenarios.md)
- [Demo script](docs/demo-script.md)
- [Known issues](docs/known-issues.md)

## Repository layout

- `sentinel/ingest/`: pure-python TLC + Open-Meteo fetchers, retry-aware
- `sentinel/assets/`: Dagster bronze + interim silver assets
- `sentinel/observability/`: Prometheus, Grafana, Loki (metrics, logs)
- `sentinel/agent/llm.py`: LiteLLM (Groq default) wrapping diagnosis prompts
- `sentinel/agent/context.py`: dbt manifest + run history + recent logs +
  similar incidents from Qdrant, all optional
- `sentinel/agent/embeddings.py`: fastembed + Qdrant for similar-incident
  retrieval
- `sentinel/agent/graph.py`: LangGraph state machine
  (classify → gather_context → diagnose → propose_action → format_incident)
- `sentinel/agent/remediation/`: three allowlisted actions
  (retry-with-backoff, partition-window-slip, coerce-to-string), each
  with guard/execute/rollback
- `sentinel/api/`: FastAPI service over the incident store + allowlist
- `sentinel/ui/dashboard.py`: Streamlit incident page
- `sentinel/chaos/`: nine failure scenarios, all wired end-to-end
- `sentinel/quality/`: incident store in sqlite for agent to consume
- `dbt/`: dbt-core on DuckDB

## Why does this exist

I wanted a project where the interesting work was the failure path, not the
happy path. Most pipelines look the same when they're green. They differ when
they're red.

## License

MIT.
