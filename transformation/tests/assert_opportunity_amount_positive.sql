select
    opportunity_id,
    amount
from {{ ref('stg_salesforce__opportunity') }}
where amount < 0