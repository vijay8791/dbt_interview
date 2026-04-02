{{
    config(
        materialized         = 'incremental',
        unique_key           = 'opportunity_history_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns',
        tags                 = ['marts', 'facts']
    )
}}

with opportunity_history as (

    select * from {{ ref('stg_salesforce__opportunity_history') }}

    {% if is_incremental() %}
        where createddate > (select max(created_at) from {{ this }})
    {% endif %}

),

opportunities as (

    select
        opportunity_id,
        name as opportunity_name,
        accountid
    from {{ ref('stg_salesforce__opportunity') }}

),

accounts as (

    select
        account_id,
        name as account_name
    from {{ ref('stg_salesforce__account') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['oh.opportunity_history_id']) }}
                                                as history_sk,
        oh.opportunity_history_id,
        oh.opportunityid                        as opportunity_id,
        opp.opportunity_name,
        acc.account_name,
        oh.stagename                            as to_stage,
        oh.fromopportunitystagename             as from_stage,
        oh.amount,
        oh.probability,
        oh.closedate,
        oh.forecastcategory,
        oh.createddate                          as created_at,
        oh.isdeleted,

        -- derived
        case
            when oh.fromopportunitystagename is null then true
            else false
        end                                     as is_first_stage,

        case
            when oh.stagename in ('Closed Won', 'Closed Lost') then true
            else false
        end                                     as is_final_stage,

        {{ generate_audit_columns() }}

    from opportunity_history    oh
    left join opportunities     opp on oh.opportunityid = opp.opportunity_id
    left join accounts          acc on opp.accountid    = acc.account_id

)

select * from final