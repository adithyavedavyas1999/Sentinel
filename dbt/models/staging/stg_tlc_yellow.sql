{{ config(materialized='view') }}

-- TLC has changed column names across vintages. We normalize here so
-- downstream models can assume tpep_* naming.
with src as (
    select * from {{ source('bronze', 'tlc_yellow') }}
),

renamed as (
    select
        cast(tpep_pickup_datetime  as timestamp) as pickup_ts,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_ts,
        cast(VendorID              as integer)   as vendor_id,
        cast(passenger_count       as integer)   as passenger_count,
        cast(trip_distance         as double)    as trip_distance,
        cast(PULocationID          as integer)   as pickup_zone_id,
        cast(DOLocationID          as integer)   as dropoff_zone_id,
        cast(payment_type          as integer)   as payment_type,
        cast(fare_amount           as double)    as fare_amount,
        cast(tip_amount            as double)    as tip_amount,
        cast(total_amount          as double)    as total_amount,
        cast(year                  as integer)   as ingest_year,
        cast(month                 as integer)   as ingest_month
    from src
)

select *
from renamed
where pickup_ts is not null
  and dropoff_ts is not null
  -- guards against the well-known TLC dirty rows: pickup in 2000-something
  -- or 2070-something. Bound by ingest partition.
  and date_trunc('month', pickup_ts) = make_date(ingest_year, ingest_month, 1)
