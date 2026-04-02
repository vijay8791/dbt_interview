{{
    config(
        materialized = 'table',
        tags = ['intermediate']
    )
}}

with opportunities as (

    select * from {{ ref('stg_salesforce__opportunity') }}

),

accounts as (

    select
        account_id,
        name              as account_name,
        industry          as account_industry,
        type              as account_type,
        rating            as account_rating,
        annualrevenue     as account_annual_revenue,
        numberofemployees as account_employee_count,
        billingcountry    as account_billing_country,
        billingstate      as account_billing_state
    from {{ ref('stg_salesforce__account') }}

),

users as (

    select
        user_id,
        firstname || ' ' || lastname as owner_full_name,
        title                        as owner_title,
        department                   as owner_department
    from {{ ref('stg_salesforce__user') }}

),

campaigns as (

    select
        campaign_id,
        name   as campaign_name,
        type   as campaign_type,
        status as campaign_status
    from {{ ref('stg_salesforce__campaign') }}

),

stage_map as (

    select * from {{ ref('seed_opportunity_stage_map') }}

),

enriched as (

    select
        -- keys
        opp.opportunity_id,
        opp.accountid,
        opp.ownerid,
        opp.campaignid,
        opp.contactid,

        -- opportunity attributes
        opp.name                        as opportunity_name,
        opp.type                        as opportunity_type,
        opp.stagename,
        opp.leadsource,
        opp.forecastcategory,
        opp.forecastcategoryname,

        -- stage enrichment from seed
        sm.stage_label,
        sm.pipeline_phase,
        sm.sort_order                   as stage_sort_order,

        -- financials
        opp.amount,
        opp.probability,
        opp.expectedrevenue,
        opp.totalopportunityquantity,

        -- flags
        opp.isclosed,
        opp.iswon,
        opp.isprivate,
        opp.hasopportunitylineitem,

        -- dates
        opp.closedate,
        opp.createddate,
        opp.lastmodifieddate,
        opp.laststagechangedate,
        opp.lastactivitydate,

        -- fiscal
        opp.fiscalyear,
        opp.fiscalquarter,

        -- account enrichment
        acc.account_name,
        acc.account_industry,
        acc.account_type,
        acc.account_rating,
        acc.account_annual_revenue,
        acc.account_employee_count,
        acc.account_billing_country,
        acc.account_billing_state,

        -- owner enrichment
        usr.owner_full_name,
        usr.owner_title,
        usr.owner_department,

        -- campaign enrichment
        cam.campaign_name,
        cam.campaign_type,
        cam.campaign_status,

        -- derived fields
        datediff('day', opp.createddate, opp.closedate) as days_to_close,

        case
            when opp.lastactivitydate is null then null
            else datediff('day', cast(opp.lastactivitydate as date), current_date)
        end                                             as days_since_last_activity,

        case
            when opp.iswon    then 'Won'
            when opp.isclosed then 'Lost'
            else 'Open'
        end                                             as opportunity_outcome,

        case
            when opp.totalopportunityquantity is null
              or cast(opp.totalopportunityquantity as double) = 0 then null
            else opp.amount / cast(opp.totalopportunityquantity as double)
        end                                             as avg_unit_price,

        -- deal size category
        case
            when opp.amount >= 100000 then 'Large'
            when opp.amount >= 10000  then 'Medium'
            when opp.amount > 0       then 'Small'
            else 'Unknown'
        end                                             as deal_size_category,

        -- is large deal flag
        case
            when opp.amount >= 100000 then true
            else false
        end                                             as is_large_deal

    from opportunities      opp
    left join accounts      acc on opp.accountid  = acc.account_id
    left join users         usr on opp.ownerid    = usr.user_id
    left join campaigns     cam on opp.campaignid = cam.campaign_id
    left join stage_map     sm  on opp.stagename  = sm.stage_name

),

-- window functions
with_window_metrics as (

    select
        *,

        -- rank opportunities by amount within each account
        row_number() over (
            partition by accountid
            order by amount desc nulls last
        )                                               as rank_by_amount_in_account,

        -- rank by recency within account
        row_number() over (
            partition by accountid
            order by createddate desc
        )                                               as rank_by_recency_in_account,

        -- total pipeline per account
        sum(amount) over (
            partition by accountid
        )                                               as total_account_pipeline,

        -- previous opportunity stage for same account
        lag(stagename) over (
            partition by accountid
            order by createddate
        )                                               as prev_opportunity_stage,

        -- running won amount per fiscal year and quarter
        sum(case when iswon then amount else 0 end) over (
            partition by fiscalyear, fiscalquarter
            order by createddate
            rows between unbounded preceding and current row
        )                                               as running_won_amount_in_quarter

    from enriched

)

select * from with_window_metrics
