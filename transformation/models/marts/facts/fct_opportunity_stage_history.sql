{{
    config(
        materialized         = 'incremental',
        unique_key           = 'opportunity_history_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns',
        tags                 = ['marts', 'facts']
    )
}}

with stage_history as (

    select * from {{ ref('int_opportunity_stage_history') }}

    {% if is_incremental() %}
        where createddate > (select max(created_at) from {{ this }})
    {% endif %}

),

final as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['opportunity_history_id']) }}
                                            as history_sk,

        -- natural key
        opportunity_history_id,

        -- foreign keys (natural)
        opportunityid                       as opportunity_id,
        ownerid                             as owner_id,

        -- context (denormalized for BI convenience)
        opportunity_name,
        account_name,
        account_industry,

        -- stage transition
        to_stage,
        from_stage,
        is_first_stage,
        is_final_stage,
        is_won_stage,

        -- measures at point in time
        amount,
        probability,
        closedate,
        forecastcategory,

        -- timing
        days_in_prev_stage,
        createddate                         as created_at,

        -- flags
        isdeleted,

        -- audit columns
        {{ generate_audit_columns() }}

    from stage_history

)

select * from final