{{ config(materialized='table') }}

-- Replaces sentinel/assets/silver/trips_weather.py. The python version
-- worked but the agent needs manifest.json + lineage, and that's free here.

with trips as (
    select * from {{ ref('stg_tlc_yellow') }}
),

weather as (
    select * from {{ ref('stg_weather_nyc_daily') }}
),

joined as (
    select
        t.*,
        cast(t.pickup_ts as date) as pickup_date,
        w.temp_max_c,
        w.temp_min_c,
        w.temp_mean_c,
        w.precipitation_mm,
        w.snowfall_cm,
        w.wind_speed_max_kmh
    from trips t
    left join weather w
        on cast(t.pickup_ts as date) = w.weather_date
)

select * from joined
