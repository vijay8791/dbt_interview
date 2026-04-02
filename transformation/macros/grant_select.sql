{% macro grant_select(schema_name) %}

    {% set schemas_to_grant = [schema_name] %}

    {% for schema in schemas_to_grant %}

        {% if target.type == 'duckdb' %}

            {{ log("DuckDB detected — skipping grant for schema: " ~ schema, info=true) }}

        {% else %}

            {% set sql %}
                grant select on all tables
                in schema {{ schema }}
                to role reporter
            {% endset %}

            {% do run_query(sql) %}
            {{ log("Granted SELECT on schema: " ~ schema ~ " to role: reporter", info=true) }}

        {% endif %}

    {% endfor %}

{% endmacro %}