{{
    config(
        materialized     = 'incremental',
        unique_key       = 'opportunity_id',
        incremental_strategy = 'merge',
        on_schema_change = 'sync_all_columns',
        tags = ['marts', 'facts', 'daily'],
        pre_hook  = "{{ log('Building fct_opportunity — incremental mode: ' ~ is_incremental(), info=true) }}",
        post_hook = "{{ log('fct_opportunity built successfully', info=true) }}"
    )
}}
with opportunity_enriched as (

    select * from {{ ref('int_opportunity_enriched') }}

    {% if is_incremental() %}
        where lastmodifieddate > (select max(last_modified_at) from {{ this }})
    {% endif %}

),

dim_account as (

    select account_id, account_sk
    from {{ ref('dim_account') }}

),

dim_user as (

    select user_id, user_sk
    from {{ ref('dim_user') }}

),

dim_campaign as (

    select campaign_id, campaign_sk
    from {{ ref('dim_campaign') }}

),

final as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['oe.opportunity_id']) }}
                                                    as opportunity_sk,

        -- natural key
        oe.opportunity_id,

        -- foreign keys (surrogate)
        da.account_sk,
        du.user_sk                                  as owner_sk,
        dc.campaign_sk,

        -- foreign keys (natural)
        oe.accountid                                as account_id,
        oe.ownerid                                  as owner_id,
        oe.campaignid                               as campaign_id,
        oe.contactid                                as contact_id,

        -- opportunity attributes
        oe.opportunity_name,
        oe.opportunity_type,
        oe.stagename,
        oe.stage_label,
        oe.pipeline_phase,
        oe.stage_sort_order,
        oe.leadsource                               as lead_source,
        oe.forecastcategory                         as forecast_category,
        oe.forecastcategoryname                     as forecast_category_name,

        -- account context
        oe.account_name,
        oe.account_industry,
        oe.account_billing_country,

        -- owner context
        oe.owner_full_name,
        oe.owner_department,

        -- campaign context
        oe.campaign_name,
        oe.campaign_type,

        -- measures
        oe.amount,
        oe.probability,
        oe.expectedrevenue                          as expected_revenue,
        oe.totalopportunityquantity                 as total_quantity,
        oe.avg_unit_price,

        -- status flags
        oe.isclosed                                 as is_closed,
        oe.iswon                                    as is_won,
        oe.isprivate                                as is_private,
        oe.opportunity_outcome,
        oe.deal_size_category,
        oe.is_large_deal,

        -- dates
        oe.closedate                                as close_date,
        oe.createddate                              as created_at,
        oe.lastmodifieddate                         as last_modified_at,
        oe.laststagechangedate                      as last_stage_change_date,

        -- fiscal
        oe.fiscalyear                               as fiscal_year,
        oe.fiscalquarter                            as fiscal_quarter,

        -- derived timing
        oe.days_to_close,
        oe.days_since_last_activity,

        -- window metrics
        oe.rank_by_amount_in_account,
        oe.rank_by_recency_in_account,
        oe.total_account_pipeline,
        oe.prev_opportunity_stage,
        oe.running_won_amount_in_quarter,

        -- audit columns
        {{ generate_audit_columns() }}

    from opportunity_enriched   oe
    left join dim_account       da  on oe.accountid  = da.account_id
    left join dim_user          du  on oe.ownerid    = du.user_id
    left join dim_campaign      dc  on oe.campaignid = dc.campaign_id

)

select * from final