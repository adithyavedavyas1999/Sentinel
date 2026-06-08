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

Weeks 1–8 done. The diagnostic agent (week 9) is the next chunk of work —
the LLM wrapper, context retrieval, and chaos harness it'll use are all
in place. See [docs/roadmap.md](docs/roadmap.md).

Screenshots: TODO. Add Dagster UI + Grafana shots once you've run the
demo end-to-end on your laptop.

## What's inside

- Dagster (assets + sensors) for orchestration
- dbt-core on DuckDB for transforms
- MinIO for raw landing, Postgres for Dagster metadata
- Prometheus + Grafana + Loki for metrics and logs
- LiteLLM (Groq default) wrapping diagnosis prompts
- fastembed + Qdrant for similar-incident retrieval
- LangGraph state machine on top — landing in week 9

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

## Docs

- [Architecture and ADRs](docs/architecture.md)
- [Roadmap](docs/roadmap.md)
- [Data model](docs/data-model.md)
- [Chaos scenarios](docs/chaos-scenarios.md)
- [Demo script](docs/demo-script.md)

## Phase 2 — in progress

Foundation is in:

- `sentinel/agent/llm.py`: LiteLLM wrapper with retry, JSON-mode validation,
  mock client for tests
- `sentinel/agent/context.py`: dbt manifest + run history + recent logs +
  similar incidents from Qdrant, all optional
- `sentinel/agent/embeddings.py`: fastembed (local, no torch) + Qdrant
- `sentinel/chaos/`: nine failure scenarios, all wired end-to-end

Still to come:

- LangGraph state machine that ties it together (week 9)
- Allowlisted auto-remediation (week 10) — see ADR-004 for what it can
  and cannot do
- FastAPI + Streamlit incident dashboard (week 11)

## Why does this exist

I wanted a project where the interesting work was the failure path, not the
happy path. Most pipelines look the same when they're green. They differ when
they're red.

## License

MIT.
