{% test sentinel_no_future_dates(model, column_name, slack_hours=24) %}
-- Allows up to `slack_hours` of clock skew between pipeline host and
-- warehouse. Defaults to 24h because partition keys are date-grain.
select *
from {{ model }}
where {{ column_name }} > current_timestamp + interval ({{ slack_hours }} hour)
{% endtest %}
