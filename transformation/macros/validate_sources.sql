-- =============================================================================
-- macro: validate_sources
-- Pre-run validation macro that checks source data quality before building.
-- Demonstrates: run_query(), log(), target, this
--
-- Usage:
--   dbt run-operation validate_sources
-- =============================================================================
{% macro validate_sources() %}

    {{ log("=========================================", info=true) }}
    {{ log("Starting source validation", info=true) }}
    {{ log("Environment : " ~ target.name, info=true) }}
    {{ log("Target type : " ~ target.type, info=true) }}
    {{ log("Schema      : " ~ target.schema, info=true) }}
    {{ log("=========================================", info=true) }}

    -- -------------------------------------------------------------------------
    -- Check 1: Row counts on key source tables
    -- -------------------------------------------------------------------------
    {% set tables_to_check = [
        'stg_salesforce__account',
        'stg_salesforce__opportunity',
        'stg_salesforce__lead',
        'stg_salesforce__case'
    ] %}

    {% for table in tables_to_check %}

        {% set count_query %}
            select count(*) as row_count
            from {{ ref(table) }}
        {% endset %}

        {% set results = run_query(count_query) %}
        {% set row_count = results.columns[0].values()[0] %}

        {{ log("Table: " ~ table ~ " → " ~ row_count ~ " rows", info=true) }}

        {% if row_count == 0 %}
            {{ log("WARNING: " ~ table ~ " has zero rows!", info=true) }}
        {% endif %}

    {% endfor %}

    {{ log("=========================================", info=true) }}

    -- -------------------------------------------------------------------------
    -- Check 2: Null rate on opportunity amount
    -- -------------------------------------------------------------------------
    {% set null_check %}
        select
            count(*) as total_rows,
            sum(case when amount is null then 1 else 0 end) as null_count
        from {{ ref('stg_salesforce__opportunity') }}
    {% endset %}

    {% set null_results = run_query(null_check) %}
    {% set total = null_results.columns[0].values()[0] %}
    {% set nulls  = null_results.columns[1].values()[0] %}

    {{ log("Opportunity amount — total: " ~ total ~ " | nulls: " ~ nulls, info=true) }}

    {% if nulls > 0 %}
        {{ log("WARNING: " ~ nulls ~ " opportunities have NULL amount", info=true) }}
    {% else %}
        {{ log("OK: No NULL amounts found", info=true) }}
    {% endif %}

    {{ log("=========================================", info=true) }}

    -- -------------------------------------------------------------------------
    -- Check 3: Environment-specific behavior using target
    -- -------------------------------------------------------------------------
    {% if target.name == 'prod' %}
        {{ log("PROD environment — strict validation enabled", info=true) }}
    {% else %}
        {{ log("DEV environment (" ~ target.name ~ ") — relaxed validation", info=true) }}
    {% endif %}

    {{ log("Source validation complete", info=true) }}
    {{ log("=========================================", info=true) }}

{% endmacro %}