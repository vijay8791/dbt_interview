-- =============================================================================
-- Analysis: lead_funnel
-- Tracks lead conversion funnel by source and industry.
-- =============================================================================

select
    lead_source,
    industry,
    count(*)                                        as total_leads,
    sum(case when is_converted then 1 else 0 end)   as converted_leads,
    sum(case when is_hot_lead  then 1 else 0 end)   as hot_leads,
    round(
        cast(sum(case when is_converted then 1 else 0 end) as double)
        / count(*) * 100,
        2
    )                                               as conversion_rate_pct,
    avg(days_to_convert)                            as avg_days_to_convert
from {{ ref('fct_lead') }}
group by 1, 2
order by total_leads desc