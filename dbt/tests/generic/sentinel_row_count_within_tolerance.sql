{% test sentinel_row_count_within_tolerance(model, expected, tolerance_pct=10) %}
-- Fails if row count is outside expected +/- tolerance_pct%.
-- Use for "we expect ~N rows" sanity checks that should warn on volume drift.
with this_count as (
    select count(*) as n from {{ model }}
)
select *
from this_count
where n < {{ expected }} * (1 - {{ tolerance_pct }} / 100.0)
   or n > {{ expected }} * (1 + {{ tolerance_pct }} / 100.0)
{% endtest %}
