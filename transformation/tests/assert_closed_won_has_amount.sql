select
    opportunity_id,
    stagename,
    amount
from {{ ref('stg_salesforce__opportunity') }}
where iswon = true
  and (amount is null or amount <= 0)