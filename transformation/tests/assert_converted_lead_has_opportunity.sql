select
    lead_id,
    isconverted,
    convertedopportunityid
from {{ ref('stg_salesforce__lead') }}
where isconverted = true
  and convertedopportunityid is null