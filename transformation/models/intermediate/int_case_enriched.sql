{{
    config(
        materialized = 'table',
        tags = ['intermediate']
    )
}}

with cases as (

    select * from {{ ref('stg_salesforce__case') }}

),

accounts as (

    select
        account_id,
        name     as account_name,
        industry as account_industry,
        type     as account_type
    from {{ ref('stg_salesforce__account') }}

),

contacts as (

    select
        contact_id,
        firstname || ' ' || lastname as contact_full_name,
        email                        as contact_email,
        phone                        as contact_phone
    from {{ ref('stg_salesforce__contact') }}

),

users as (

    select
        user_id,
        firstname || ' ' || lastname as owner_full_name,
        department                   as owner_department
    from {{ ref('stg_salesforce__user') }}

),

case_history as (

    select
        caseid,
        count(*)              as total_status_changes,
        min(lastmodifieddate) as first_status_change_date,
        max(lastmodifieddate) as last_status_change_date
    from {{ ref('stg_salesforce__case_history_2') }}
    group by 1

),

enriched as (

    select
        -- keys
        c.case_id,
        c.accountid,
        c.contactid,
        c.ownerid,
        c.productid,

        -- case attributes
        c.casenumber,
        c.type          as case_type,
        c.status        as case_status,
        c.reason        as case_reason,
        c.origin        as case_origin,
        c.subject,
        c.priority,

        -- flags
        c.isclosed,
        c.isescalated,

        -- dates
        c.createddate,
        c.closeddate,
        c.lastmodifieddate,
        c.slastartdate,
        c.slaexitdate,

        -- sla
        c.slaviolation__c               as is_sla_violation,

        -- account enrichment
        acc.account_name,
        acc.account_industry,
        acc.account_type,

        -- contact enrichment
        con.contact_full_name,
        con.contact_email,
        con.contact_phone,

        -- owner enrichment
        usr.owner_full_name,
        usr.owner_department,

        -- history enrichment
        ch.total_status_changes,
        ch.first_status_change_date,
        ch.last_status_change_date,

        -- derived fields
        datediff('hour', c.createddate, c.closeddate) as hours_to_resolve,
        datediff('day',  c.createddate, c.closeddate) as days_to_resolve,

        case
            when c.isclosed and c.slaviolation__c = 'false' then true
            when c.isclosed                                  then false
            else null
        end                                           as resolved_within_sla,

        -- is high priority flag
        case
            when c.priority in ('High', 'Critical') then true
            else false
        end                                           as is_high_priority,

        -- resolution speed category
        case
            when datediff('hour', c.createddate, c.closeddate) <= 4  then 'Fast'
            when datediff('hour', c.createddate, c.closeddate) <= 24 then 'Normal'
            when datediff('hour', c.createddate, c.closeddate) > 24  then 'Slow'
            else 'Open'
        end                                           as resolution_speed

    from cases          c
    left join accounts  acc on c.accountid = acc.account_id
    left join contacts  con on c.contactid = con.contact_id
    left join users     usr on c.ownerid   = usr.user_id
    left join case_history ch on c.case_id = ch.caseid

),

-- window functions
with_window_metrics as (

    select
        *,

        -- rank cases by recency within each account
        row_number() over (
            partition by accountid
            order by createddate desc
        )                                           as case_recency_rank_in_account,

        -- total cases per account
        count(*) over (
            partition by accountid
        )                                           as total_cases_for_account,

        -- total cases per account per type
        count(*) over (
            partition by accountid, case_type
        )                                           as cases_by_type_for_account,

        -- closed cases per account
        sum(case when isclosed then 1 else 0 end) over (
            partition by accountid
        )                                           as closed_cases_for_account,

        -- previous case status for same account
        lag(case_status) over (
            partition by accountid
            order by createddate
        )                                           as prev_case_status

    from enriched

)

select * from with_window_metrics
