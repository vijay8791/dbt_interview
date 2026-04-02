-- Test: Every converted lead must have a converted opportunity ID.
-- A converted lead with no opportunity link is incomplete data.

select
    lead_id,
    lead_full_name,
    is_converted,
    converted_opportunity_id
from {{ ref('fct_lead') }}
where is_converted = true
  and converted_opportunity_id is null