# Chaos scenarios

> Rewrite the prose in your own voice before publishing.

The chaos harness exists so we can grade the Phase 2 agent against a fixed
set of reproducible failures. Each scenario lists what it does, where it
should be caught, and what an ideal agent response looks like.

Run with: `sentinel chaos inject <scenario>` (`--dry-run` to print the plan).

## Scenarios

### `tlc_5xx`

**What:** intercept the next TLC fetch and return 503 a few times.
**Detected at:** `bronze.tlc_yellow` materialization — tenacity retries kick
in, then bubbles up after attempts exhausted.
**Ideal agent response:** retry-with-backoff (allowlisted, safe). Auto-heal.

### `tlc_schema_drift`

**What:** rewrite the most recent bronze TLC parquet to rename a column.
**Detected at:** `bronze.tlc_yellow` metadata: `schema_fingerprint` changes.
Also caught at `stg_tlc_yellow` since the cast on the renamed column fails.
**Ideal agent response:** coerce the renamed column to a known mapping if
the rename is in our alias map; otherwise file incident + propose mapping.

### `weather_429`

**What:** force the Open-Meteo fetcher to see 429 responses.
**Detected at:** `bronze.weather_nyc_daily` — tenacity backs off.
**Ideal agent response:** retry-with-backoff. No remediation needed beyond
that. Useful as a "everything's fine" negative control.

### `duckdb_lock`

**What:** hold a write lock on the duckdb file from a side process.
**Detected at:** dbt run fails with "could not get exclusive lock".
**Ideal agent response:** wait and retry (lock is transient). If repeated,
file incident — something is misconfigured.

### `dbt_sql_error`

**What:** push a broken model SQL into a feature branch and run dbt.
**Detected at:** dbt parse/compile fails before run.
**Ideal agent response:** *no remediation*. SQL errors mean code is wrong;
the agent files an incident with the dbt log excerpt and proposed-fix text.
This is the negative case — we test that the agent does NOT try to fix SQL.

### `null_spike`

**What:** stuff nulls into `pickup_ts` for a slice of bronze.
**Detected at:** dbt `not_null` test on `stg_tlc_yellow.pickup_ts`.
**Ideal agent response:** quarantine the bad rows into a `_quarantine/` bucket
prefix; let the rest of the partition through. Allowlisted as
"schema-drift coerce" with quarantine.

### `volume_drop`

**What:** rewrite the latest bronze TLC parquet keeping only the first ~5%
of rows. Destructive — re-run the matching ingest partition to restore.
**Detected at:** `fct_trips_daily` row-count tolerance test (warn). Optional
gold-level volume anomaly check.
**Ideal agent response:** file incident. Don't auto-heal — this could be
legitimate (e.g. holiday) or a real bug.

### `late_partition`

**What:** flag-based; the next `bronze.tlc_yellow` materialization raises
`ChaosTriggered` with a "partition not yet published upstream (404)"
message. Stand-in for the real failure mode where TLC hasn't dropped
the month yet.
**Detected at:** `bronze.tlc_yellow` materialization.
**Ideal agent response:** partition window slip (allowlisted) — use D-1.

## Detection coverage matrix

|                       | bronze metadata | asset check | dbt test | gold check |
|-----------------------|:---------------:|:-----------:|:--------:|:----------:|
| tlc_5xx               |        x        |             |          |            |
| tlc_schema_drift      |        x        |             |    x     |            |
| weather_429           |        x        |             |          |            |
| duckdb_lock           |                 |             |    x     |            |
| dbt_sql_error         |                 |             |    x     |            |
| null_spike            |                 |      x      |    x     |            |
| volume_drop           |                 |      x      |          |     x      |
| late_partition        |        x        |             |          |            |

> Status: all nine scenarios are wired end-to-end as of week 7. The two
> previously-stubbed entries (`volume_drop`, `late_partition`) now have
> real implementations — the agent eval suite in phase 2 will use them
> as ground-truth fixtures.

If a row has no x, we don't catch it yet. Worth adding before Phase 2.
