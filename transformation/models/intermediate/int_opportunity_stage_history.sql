{{
    config(
        materialized = 'table',
        tags         = ['intermediate']
    )
}}

with opportunity_history as (

    select * from {{ ref('stg_salesforce__opportunity_history') }}

),

opportunities as (

    select
        opportunity_id,
        name        as opportunity_name,
        accountid,
        ownerid,
        stagename   as current_stage
    from {{ ref('stg_salesforce__opportunity') }}

),

accounts as (

    select
        account_id,
        name     as account_name,
        industry as account_industry
    from {{ ref('stg_salesforce__account') }}

),

enriched as (

    select
        -- keys
        oh.opportunity_history_id,
        oh.opportunityid,
        opp.opportunity_name,
        opp.ownerid,

        -- account context
        acc.account_name,
        acc.account_industry,

        -- stage transition
        oh.stagename                        as to_stage,
        oh.fromopportunitystagename         as from_stage,

        -- measures at point in time
        oh.amount,
        oh.probability,
        oh.closedate,
        oh.forecastcategory,

        -- history metadata
        oh.createddate,
        oh.isdeleted,

        -- derived fields
        case
            when oh.fromopportunitystagename is null then true
            else false
        end                                 as is_first_stage,

        case
            when oh.stagename in ('Closed Won', 'Closed Lost') then true
            else false
        end                                 as is_final_stage,

        case
            when oh.stagename = 'Closed Won'  then true
            else false
        end                                 as is_won_stage,

        -- days spent in the previous stage before this transition
        datediff(
            'day',
            lag(oh.createddate) over (
                partition by oh.opportunityid
                order by oh.createddate
            ),
            oh.createddate
        )                                   as days_in_prev_stage

    from opportunity_history    oh
    left join opportunities     opp on oh.opportunityid = opp.opportunity_id
    left join accounts          acc on opp.accountid    = acc.account_id

)

select * from enriched