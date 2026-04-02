{{
    config(
        materialized = 'table',
        tags = ['marts', 'facts', 'daily']
    )
}}

with case_enriched as (

    select * from {{ ref('int_case_enriched') }}

),

dim_account as (

    select account_id, account_sk
    from {{ ref('dim_account') }}

),

dim_contact as (

    select contact_id, contact_sk
    from {{ ref('dim_contact') }}

),

dim_user as (

    select user_id, user_sk
    from {{ ref('dim_user') }}

),

final as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['ce.case_id']) }}
                                                    as case_sk,

        -- natural key
        ce.case_id,
        ce.casenumber                               as case_number,

        -- foreign keys (surrogate)
        da.account_sk,
        dc.contact_sk,
        du.user_sk                                  as owner_sk,

        -- foreign keys (natural)
        ce.accountid                                as account_id,
        ce.contactid                                as contact_id,
        ce.ownerid                                  as owner_id,
        ce.productid                                as product_id,

        -- case attributes
        ce.case_type,
        ce.case_status,
        ce.case_reason,
        ce.case_origin,
        ce.subject,
        ce.priority,

        -- account context
        ce.account_name,
        ce.account_industry,

        -- contact context
        ce.contact_full_name,
        ce.contact_email,

        -- owner context
        ce.owner_full_name,
        ce.owner_department,

        -- status flags
        ce.isclosed                                 as is_closed,
        ce.isescalated                              as is_escalated,
        ce.is_sla_violation,
        ce.resolved_within_sla,
        ce.is_high_priority,
        ce.resolution_speed,

        -- dates
        ce.createddate                              as created_at,
        ce.closeddate                               as closed_at,
        ce.lastmodifieddate                         as last_modified_at,
        ce.slastartdate                             as sla_start_date,
        ce.slaexitdate                              as sla_exit_date,

        -- timing measures
        ce.hours_to_resolve,
        ce.days_to_resolve,

        -- history metrics
        ce.total_status_changes,
        ce.first_status_change_date,
        ce.last_status_change_date,

        -- window metrics
        ce.case_recency_rank_in_account,
        ce.total_cases_for_account,
        ce.cases_by_type_for_account,
        ce.closed_cases_for_account,
        ce.prev_case_status,

        -- audit columns
        {{ generate_audit_columns() }}

    from case_enriched          ce
    left join dim_account       da  on ce.accountid = da.account_id
    left join dim_contact       dc  on ce.contactid = dc.contact_id
    left join dim_user          du  on ce.ownerid   = du.user_id

)

select * from final