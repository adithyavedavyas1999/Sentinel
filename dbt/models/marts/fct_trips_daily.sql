{{ config(materialized='table') }}

with base as (
    select * from {{ ref('silver_trips_weather') }}
)

select
    pickup_date,
    pickup_zone_id,
    count(*)                                  as trip_count,
    sum(passenger_count)                      as passengers,
    avg(trip_distance)                        as avg_distance,
    avg(total_amount)                         as avg_fare,
    avg(case when tip_amount > 0 then 1.0 else 0.0 end) as tip_rate,
    -- weather is the same value across rows on a given date, so any() works
    max(temp_max_c)        as temp_max_c,
    max(temp_min_c)        as temp_min_c,
    max(precipitation_mm)  as precipitation_mm,
    max(snowfall_cm)       as snowfall_cm
from base
group by pickup_date, pickup_zone_id
