-- =============================================================================
-- Generic test: is_between
-- Checks that all non-null values in a column fall within min and max range.
--
-- Usage in YAML:
--   tests:
--     - is_between:
--         min_value: 0
--         max_value: 100
-- =============================================================================
{% test is_between(model, column_name, min_value, max_value) %}

select
    {{ column_name }} as failing_value
from {{ model }}
where {{ column_name }} is not null
  and (
      {{ column_name }} < {{ min_value }}
      or {{ column_name }} > {{ max_value }}
  )

{% endtest %}