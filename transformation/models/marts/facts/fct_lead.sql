{{
    config(
        materialized = 'table',
        tags = ['marts', 'facts', 'daily']
    )
}}

with lead_enriched as (

    select * from {{ ref('int_lead_enriched') }}

),

dim_user as (

    select user_id, user_sk
    from {{ ref('dim_user') }}

),

final as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['le.lead_id']) }}
                                                    as lead_sk,

        -- natural key
        le.lead_id,

        -- foreign keys (surrogate)
        du.user_sk                                  as owner_sk,

        -- foreign keys (natural)
        le.ownerid                                  as owner_id,
        le.convertedaccountid                       as converted_account_id,
        le.convertedcontactid                       as converted_contact_id,
        le.convertedopportunityid                   as converted_opportunity_id,

        -- lead attributes
        le.lead_full_name,
        le.title,
        le.company,
        le.industry,
        le.leadsource                               as lead_source,
        le.lead_status,
        le.status_label,
        le.status_sort_order,
        le.rating,
        le.product_interest,

        -- financials
        le.annualrevenue                            as annual_revenue,
        le.numberofemployees                        as number_of_employees,

        -- location
        le.city,
        le.state,
        le.country,

        -- contact info
        le.email,
        le.phone,

        -- owner context
        le.owner_full_name,
        le.owner_department,

        -- flags
        le.isconverted                              as is_converted,
        le.hasoptedoutofemail                       as has_opted_out_of_email,
        le.donotcall                                as do_not_call,
        le.is_hot_lead,
        le.has_email,
        le.has_phone,

        -- lifecycle
        le.lead_lifecycle_stage,

        -- dates
        le.createddate                              as created_at,
        le.converteddate                            as converted_at,
        le.lastmodifieddate                         as last_modified_at,
        le.lastactivitydate                         as last_activity_at,

        -- timing measures
        le.days_to_convert,

        -- window metrics
        le.recency_rank_for_owner,
        le.total_leads_for_owner,
        le.converted_leads_for_owner,
        le.total_leads_from_company,
        le.prev_lead_status_for_owner,
        le.recency_rank_in_industry_source,

        -- derived conversion rate
        case
            when le.total_leads_for_owner = 0
              or le.total_leads_for_owner is null then null
            else round(
                cast(le.converted_leads_for_owner as double)
                / cast(le.total_leads_for_owner as double) * 100,
                2
            )
        end                                         as owner_conversion_rate_pct,

        -- audit columns
        {{ generate_audit_columns() }}

    from lead_enriched              le
    left join dim_user              du  on le.ownerid = du.user_id

)

select * from final