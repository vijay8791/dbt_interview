-- =============================================================================
-- Analysis: pipeline_summary
-- Ad-hoc query summarising current opportunity pipeline by stage and account.
-- Run with: dbt compile --select pipeline_summary
-- Then find compiled SQL in target/compiled/
-- =============================================================================

with pipeline as (

    select
        pipeline_phase,
        stage_label,
        account_name,
        account_industry,
        count(*)                        as opportunity_count,
        sum(amount)                     as total_amount,
        avg(amount)                     as avg_amount,
        sum(case when is_won then amount else 0 end)
                                        as won_amount,
        sum(case when is_closed and not is_won then 1 else 0 end)
                                        as lost_count
    from {{ ref('fct_opportunity') }}
    where is_closed = false
    group by 1, 2, 3, 4

)

select
    pipeline_phase,
    stage_label,
    account_name,
    account_industry,
    opportunity_count,
    total_amount,
    avg_amount,
    won_amount
from pipeline
order by
    case pipeline_phase
        when 'Early' then 1
        when 'Mid'   then 2
        when 'Late'  then 3
        else 4
    end,
    total_amount desc