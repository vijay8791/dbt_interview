-- Test: Every closed case must have a closed_at date.
-- A case marked closed with no closure date is a data quality issue.

select
    case_id,
    case_number,
    is_closed,
    closed_at
from {{ ref('fct_case') }}
where is_closed = true
  and closed_at is null