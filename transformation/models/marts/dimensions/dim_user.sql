{{
    config(
        materialized = 'table',
        tags = ['marts', 'dimensions']
    )
}}

with users as (

    select * from {{ ref('stg_salesforce__user') }}

),

user_roles as (

    select
        user_role_id,
        name as role_name
    from {{ ref('stg_salesforce__user_role') }}

),

-- find managers — users who appear as managerid of another user
managers as (

    select distinct managerid as user_id
    from {{ ref('stg_salesforce__user') }}
    where managerid is not null

),

final as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['u.user_id']) }} as user_sk,

        -- natural key
        u.user_id,

        -- identity
        u.username,
        u.firstname                         as first_name,
        u.lastname                          as last_name,
        coalesce(
            u.firstname || ' ' || u.lastname,
            u.lastname,
            u.firstname,
            u.username
        )                                   as full_name,
        u.alias,
        u.title,
        u.department,
        u.division,
        u.companyname                       as company_name,
        u.employeenumber                    as employee_number,

        -- contact
        u.email,
        u.phone,
        u.mobilephone                       as mobile_phone,

        -- type
        u.usertype                          as user_type,
        u.isactive                          as is_active,
        u.forecastenabled                   as is_forecast_enabled,

        -- locale
        u.timezonesidkey                    as timezone,
        u.localesidkey                      as locale,
        u.languagelocalekey                 as language,

        -- role
        u.userroleid                        as user_role_id,
        ur.role_name,

        -- hierarchy
        u.managerid                         as manager_id,
        u.profileid                         as profile_id,

        -- activity
        u.lastlogindate                     as last_login_at,
        u.createddate                       as created_at,
        u.lastmodifieddate                  as last_modified_at,

        -- derived fields
        case
            when m.user_id is not null then true
            else false
        end                                 as is_manager,

        case
            when u.lastlogindate is null then null
            else datediff('day', cast(u.lastlogindate as date), current_date)
        end                                 as days_since_last_login,

        case
            when u.isactive = true
             and u.usertype = 'Standard' then true
            else false
        end                                 as is_standard_active_user

    from users          u
    left join user_roles ur on u.userroleid = ur.user_role_id
    left join managers   m  on u.user_id    = m.user_id

)

select * from final