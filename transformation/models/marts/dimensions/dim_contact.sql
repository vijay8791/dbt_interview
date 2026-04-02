{{
    config(
        materialized = 'table',
        tags = ['marts', 'dimensions']
    )
}}

with contacts as (

    select * from {{ ref('stg_salesforce__contact') }}

),

accounts as (

    select
        account_id,
        name as account_name
    from {{ ref('stg_salesforce__account') }}

),

final as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['c.contact_id']) }} as contact_sk,

        -- natural key
        c.contact_id,

        -- identity
        c.salutation,
        c.firstname                             as first_name,
        c.lastname                              as last_name,
        c.firstname || ' ' || c.lastname        as full_name,
        c.title,
        c.department,

        -- contact info
        c.email,
        c.phone,
        c.mobilephone                           as mobile_phone,

        -- address
        c.mailingstreet                         as mailing_street,
        c.mailingcity                           as mailing_city,
        c.mailingstate                          as mailing_state,
        c.mailingpostalcode                     as mailing_postal_code,
        c.mailingcountry                        as mailing_country,

        -- lead info
        c.leadsource                            as lead_source,
        c.birthdate,

        -- preference flags
        c.hasoptedoutofemail                    as has_opted_out_of_email,
        c.hasoptedoutoffax                      as has_opted_out_of_fax,
        c.donotcall                             as do_not_call,

        -- custom fields
        c.level__c                              as contact_level,
        c.languages__c                          as languages,

        -- relationships
        c.accountid                             as account_id,
        acc.account_name,
        c.ownerid                               as owner_id,
        c.reportstoid                           as reports_to_id,

        -- metadata
        c.createddate                           as created_at,
        c.lastmodifieddate                      as last_modified_at,

        -- derived fields
        case
            when c.email is not null
             and c.email != ''
             and c.hasoptedoutofemail = false
             and c.donotcall = false then true
            else false
        end                                     as is_contactable,

        case
            when c.email is not null
             and c.email != ''
             and c.hasoptedoutofemail = false then 'Email'
            when c.phone is not null
             and c.phone != ''
             and c.donotcall = false then 'Phone'
            when c.mobilephone is not null
             and c.mobilephone != '' then 'Mobile'
            else 'None'
        end                                     as preferred_contact_method,

        case
            when c.email is not null
             and c.email != '' then true
            else false
        end                                     as has_email,

        case
            when c.phone is not null
             and c.phone != '' then true
            else false
        end                                     as has_phone

    from contacts   c
    left join accounts acc on c.accountid = acc.account_id

)

select * from final