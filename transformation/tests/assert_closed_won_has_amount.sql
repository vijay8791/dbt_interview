select
    opportunity_id,
    opportunity_outcome,
    amount
from {{ ref('fct_opportunity') }}
where is_won = true
  and (amount is null or amount <= 0)