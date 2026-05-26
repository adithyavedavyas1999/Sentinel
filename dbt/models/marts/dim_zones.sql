{{ config(materialized='table') }}

-- Zone dimension comes from a checked-in seed. TLC publishes the lookup
-- as CSV; we vendored a trimmed version. Refresh annually.
select
    cast(LocationID as integer) as zone_id,
    cast(Borough    as varchar) as borough,
    cast(Zone       as varchar) as zone_name,
    cast(service_zone as varchar) as service_zone
from {{ ref('zones') }}
