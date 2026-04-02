{{
    config(
        materialized = 'table',
        tags = ['marts', 'dimensions']
    )
}}

with accounts as (

    select * from {{ ref('stg_salesforce__account') }}

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
        {{ dbt_utils.generate_surrogate_key(['a.account_id']) }} as account_sk,

        -- natural key
        a.account_id,

        -- account attributes
        a.name                  as account_name,
        a.type                  as account_type,
        a.industry,
        a.rating,
        a.website,
        a.accountnumber         as account_number,
        a.ownership,
        a.tickersymbol          as ticker_symbol,
        a.description,

        -- financials
        a.annualrevenue         as annual_revenue,
        a.numberofemployees     as number_of_employees,

        -- location
        a.billingstreet         as billing_street,
        a.billingcity           as billing_city,
        a.billingstate          as billing_state,
        a.billingpostalcode     as billing_postal_code,
        a.billingcountry        as billing_country,

        -- sla custom fields
        a.sla__c                as sla_tier,
        a.slaexpirationdate__c  as sla_expiration_date,
        a.slaserialnumber__c    as sla_serial_number,
        a.customerpriority__c   as customer_priority,
        a.active__c             as is_active,

        -- relationships
        a.ownerid               as owner_id,
        u.owner_full_name,
        a.parentid              as parent_account_id,

        -- metadata
        a.createddate           as created_at,
        a.lastmodifieddate      as last_modified_at,

        -- derived fields
        case
            when a.annualrevenue >= 1000000000 then 'Enterprise'
            when a.annualrevenue >= 100000000  then 'Mid-Market'
            when a.annualrevenue >= 10000000   then 'SMB'
            else 'Other'
        end                     as account_tier,

        case
            when a.annualrevenue >= 1000000000 then true
            else false
        end                     as is_enterprise,

        case
            when a.sla__c is not null
             and a.sla__c != '' then true
            else false
        end                     as has_sla,

        case
            when a.numberofemployees >= 1000 then 'Large'
            when a.numberofemployees >= 100  then 'Medium'
            when a.numberofemployees > 0     then 'Small'
            else 'Unknown'
        end                     as company_size

    from accounts   a
    left join users u on a.ownerid = u.user_id

)

select * from final