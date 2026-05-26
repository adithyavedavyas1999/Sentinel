# Data model

> Rewrite this in your own voice before committing.

Layers map to the medallion. Bronze is land-as-is, silver is one-row-per-event
cleaned, gold is business grain.

## Bronze (object store)

Hive-partitioned parquet in MinIO. DuckDB reads via httpfs.

| Asset                       | Layout                                                          |
|-----------------------------|-----------------------------------------------------------------|
| `bronze.tlc_yellow`         | `bronze/tlc/yellow/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet` |
| `bronze.weather_nyc_daily`  | `bronze/weather/nyc/year=YYYY/month=MM/weather_nyc_YYYY-MM.parquet`    |

Schemas are TLC-defined (yellow) and Open-Meteo-defined (weather). We don't
rename anything in bronze — that's silver's job.

## Silver (dbt views)

### `stg_tlc_yellow`

Normalized trip rows. Casts to consistent types. Strips out the well-known
out-of-window pickup_ts dirt by filtering to the ingest month.

| column          | type      | notes                                           |
|-----------------|-----------|-------------------------------------------------|
| pickup_ts       | timestamp | not null                                        |
| dropoff_ts      | timestamp | not null                                        |
| vendor_id       | integer   |                                                 |
| passenger_count | integer   | nullable                                        |
| trip_distance   | double    | miles                                           |
| pickup_zone_id  | integer   | FK to dim_zones (warn-level, not all populated) |
| dropoff_zone_id | integer   |                                                 |
| payment_type    | integer   | 1=credit, 2=cash, ...                           |
| fare_amount     | double    | USD                                             |
| tip_amount      | double    | USD                                             |
| total_amount    | double    | USD                                             |

### `stg_weather_nyc_daily`

| column              | type   | notes                          |
|---------------------|--------|--------------------------------|
| weather_date        | date   | unique, not null               |
| temp_max_c          | double | celsius                        |
| temp_min_c          | double |                                |
| temp_mean_c         | double |                                |
| precipitation_mm    | double | sum over the day               |
| rain_mm             | double | subset of precipitation_mm     |
| snowfall_cm         | double |                                |
| wind_speed_max_kmh  | double |                                |
| wind_gust_max_kmh   | double |                                |

### `silver_trips_weather` (mart)

Trip-grain. `stg_tlc_yellow` left joined to `stg_weather_nyc_daily` on
`cast(pickup_ts as date) = weather_date`. Replaces the throwaway Python
silver from week 2.

## Gold (dbt tables)

### `fct_trips_daily`

One row per `(pickup_date, pickup_zone_id)`. Aggregates ride volume and
attaches the same weather attributes from silver. Use for the daily demand
+ weather correlation work and for the volume-anomaly DQ checks.

### `dim_zones`

Seeded from `dbt/seeds/zones.csv`. Refresh annually when TLC publishes
updates. Only ~70 zones in the seed — enough to demonstrate the join, not
the full ~265.

## Layer contracts (informal)

- Bronze: never deleted, never mutated. The agent's auto-remediation may
  *quarantine* into `_quarantine/` subkeys but not touch original objects.
- Silver: idempotent on `(pickup_ts, vendor_id, pickup_zone_id)` — rebuilding
  yields the same rows.
- Gold: depends only on silver. Never reads bronze directly.
