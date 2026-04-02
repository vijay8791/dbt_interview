{% macro generate_audit_columns() %}
    {% set audit_columns = [
        ('current_timestamp::timestamp',  'dbt_loaded_at'),
        ("'" ~ this.name ~ "'",           'dbt_model_name'),
        ("'" ~ target.name ~ "'",         'dbt_environment'),
        ('current_date',                  'dbt_loaded_date')
    ] %}
    {% for expression, alias in audit_columns %}
    {{ expression }} as {{ alias }}
        {%- if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}
