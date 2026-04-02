{{
    config(
        materialized = 'table',
        tags = ['marts', 'dimensions']
    )
}}

{{
    dbt_date.get_date_dimension(
        start_date = '2018-01-01',
        end_date   = '2030-12-31'
    )
}}