-- Test: No opportunity should have a close date more than 5 years in the future.
-- Dates beyond this are likely data entry errors.

select
    opportunity_id,
    opportunity_name,
    close_date
from {{ ref('fct_opportunity') }}
where close_date > current_date + interval '5 years'