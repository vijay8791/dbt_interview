{{
    config(
        materialized = 'table',
        tags = ['marts', 'dimensions']
    )
}}

with campaigns as (

    select * from {{ ref('stg_salesforce__campaign') }}

),

users as (

    select
        user_id,
        firstname || ' ' || lastname as owner_full_name
    from {{ ref('stg_salesforce__user') }}

),

final as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['c.campaign_id']) }} as campaign_sk,

        -- natural key
        c.campaign_id,

        -- attributes
        c.name              as campaign_name,
        c.type              as campaign_type,
        c.status            as campaign_status,
        c.isactive          as is_active,
        c.description,

        -- dates
        c.startdate         as start_date,
        c.enddate           as end_date,

        -- budget
        c.budgetedcost      as budgeted_cost,
        c.actualcost        as actual_cost,
        c.expectedrevenue   as expected_revenue,
        c.numbersent        as number_sent,

        -- performance metrics
        c.numberofleads             as number_of_leads,
        c.numberofconvertedleads    as number_of_converted_leads,
        c.numberofcontacts          as number_of_contacts,
        c.numberofresponses         as number_of_responses,
        c.numberofopportunities     as number_of_opportunities,
        c.numberofwonopportunities  as number_of_won_opportunities,
        c.amountallopportunities    as amount_all_opportunities,
        c.amountwonopportunities    as amount_won_opportunities,

        -- relationships
        c.ownerid           as owner_id,
        u.owner_full_name,
        c.parentid          as parent_campaign_id,

        -- metadata
        c.createddate       as created_at,
        c.lastmodifieddate  as last_modified_at,

        -- derived fields
        case
            when c.budgetedcost is null
              or c.budgetedcost = 0 then null
            else round(
                (c.amountwonopportunities - c.actualcost) / c.budgetedcost * 100,
                2
            )
        end                         as campaign_roi_pct,

        case
            when c.budgetedcost is null
              or c.budgetedcost = 0 then null
            else round(c.actualcost / c.budgetedcost * 100, 2)
        end                         as budget_utilization_pct,

        case
            when c.actualcost > c.budgetedcost then true
            else false
        end                         as is_over_budget,

        case
            when c.numberofleads = 0
              or c.numberofleads is null then null
            else round(
                cast(c.numberofconvertedleads as double)
                / cast(c.numberofleads as double) * 100,
                2
            )
        end                         as lead_conversion_rate_pct,

        case
            when c.numberofopportunities = 0
              or c.numberofopportunities is null then null
            else round(
                cast(c.numberofwonopportunities as double)
                / cast(c.numberofopportunities as double) * 100,
                2
            )
        end                         as opportunity_win_rate_pct

    from campaigns  c
    left join users u on c.ownerid = u.user_id

)

select * from final