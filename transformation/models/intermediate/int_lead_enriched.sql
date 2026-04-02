{{
    config(
        materialized = 'table',
        tags = ['intermediate']
    )
}}

with leads as (

    select * from {{ ref('stg_salesforce__lead') }}

),

users as (

    select
        user_id,
        firstname || ' ' || lastname as owner_full_name,
        department                   as owner_department
    from {{ ref('stg_salesforce__user') }}

),

status_map as (

    select * from {{ ref('seed_lead_status_map') }}

),

enriched as (

    select
        -- keys
        l.lead_id,
        l.ownerid,
        l.convertedaccountid,
        l.convertedcontactid,
        l.convertedopportunityid,

        -- lead attributes
        l.salutation,
        l.firstname,
        l.lastname,
        l.firstname || ' ' || l.lastname  as lead_full_name,
        l.title,
        l.company,
        l.industry,
        l.leadsource,
        l.status                           as lead_status,
        l.rating,
        l.annualrevenue,
        l.numberofemployees,
        l.productinterest__c               as product_interest,
        l.currentgenerators__c             as current_generators,
        l.numberoflocations__c             as number_of_locations,

        -- contact details
        l.email,
        l.phone,
        l.city,
        l.state,
        l.country,

        -- flags
        l.isconverted,
        l.isdeleted,
        l.hasoptedoutofemail,
        l.donotcall,

        -- dates
        l.createddate,
        l.converteddate,
        l.lastmodifieddate,
        l.lastactivitydate,

        -- owner enrichment
        usr.owner_full_name,
        usr.owner_department,

        -- status enrichment from seed
        sm.status_label,
        sm.is_active        as status_is_active,
        sm.is_converted     as status_is_converted,
        sm.sort_order       as status_sort_order,

        -- derived fields
        case
            when l.converteddate is null then null
            else datediff('day', l.createddate, cast(l.converteddate as timestamp))
        end                              as days_to_convert,

        case
            when l.isconverted then 'Converted'
            when l.isdeleted   then 'Deleted'
            else 'Active'
        end                              as lead_lifecycle_stage,

        -- is hot lead flag
        case
            when l.rating = 'Hot'
             and l.lastactivitydate is not null then true
            else false
        end                              as is_hot_lead,

        -- has email flag
        case
            when l.email is not null
             and l.email != '' then true
            else false
        end                              as has_email,

        -- has phone flag
        case
            when l.phone is not null
             and l.phone != '' then true
            else false
        end                              as has_phone

    from leads          l
    left join users     usr on l.ownerid  = usr.user_id
    left join status_map sm on l.status   = sm.lead_status

),

-- window functions
with_window_metrics as (

    select
        *,

        -- rank leads by recency within owner
        row_number() over (
            partition by ownerid
            order by createddate desc
        )                                               as recency_rank_for_owner,

        -- total leads per owner
        count(*) over (
            partition by ownerid
        )                                               as total_leads_for_owner,

        -- converted leads per owner
        sum(case when isconverted then 1 else 0 end) over (
            partition by ownerid
        )                                               as converted_leads_for_owner,

        -- total leads per company
        count(*) over (
            partition by company
        )                                               as total_leads_from_company,

        -- previous lead status for same owner
        lag(lead_status) over (
            partition by ownerid
            order by createddate
        )                                               as prev_lead_status_for_owner,

        -- rank leads by recency within industry and source
        row_number() over (
            partition by industry, leadsource
            order by createddate desc
        )                                               as recency_rank_in_industry_source

    from enriched

)

select * from with_window_metrics
