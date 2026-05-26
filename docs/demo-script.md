# Demo script

> Rewrite in your own voice before recording.

Target: 4-6 minutes. Don't over-script — it'll sound rehearsed. Goal is to
show the failure path, because that's what's interesting.

## Setup (do this before recording)

```bash
make down && docker volume rm sentinel_sentinel-warehouse || true
make up
sleep 30   # let the stack settle
make seed-data
```

Confirm:

- Dagster UI at <http://localhost:3000> shows the asset graph
- Grafana at <http://localhost:3001> shows the Sentinel dashboards
- MinIO console at <http://localhost:9001> is reachable

## Recording

### 0:00 — context

"This is Sentinel. Local data pipeline, NYC taxi data joined with NYC
weather, with a failure-handling loop on top. The interesting part isn't
the data — it's what happens when the pipeline breaks."

### 0:30 — the happy path

Materialize 3 months of bronze + silver + gold from the Dagster UI. Show
the asset graph, show one materialization green-checking.

Open Grafana, show Pipeline Health: materialization counts, ingest latency.

### 2:00 — break something

Run `sentinel chaos inject tlc_schema_drift --dry-run` first to show what
it'll do. Then run for real.

Re-materialize `stg_tlc_yellow`. It fails — dbt complains about the cast.

### 3:00 — failure capture

Show the failure in the Dagster UI. Then:

```bash
sentinel incident list
```

There's the incident, captured with structured context (asset, error type,
upstream lineage, recent metadata). Show `sentinel incident show <id>` for
the full record.

"This is the input the Phase 2 agent reads."

### 4:00 — observability

Back to Grafana. Show Data Quality dashboard: the check that failed shows
up as a downward kink in pass rate. Click into the Loki panel to see the
error log line in context.

### 4:30 — what's next

"This is Phase 1. Phase 2 wires a LangGraph agent to those incidents.
For a small allowlist of failure modes — like retry, partition slip,
schema-drift quarantine — the agent will auto-remediate. For anything
else, it files an incident with a proposed fix. The point is the audit
trail, not the AI magic."

End.
