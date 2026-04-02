{{
    config(
        materialized = 'table',
        tags = ['marts', 'dimensions']
    )
}}

with products as (

    select * from {{ ref('stg_salesforce__product_2') }}

),

pricebook_entries as (

    select
        product2id,
        min(unitprice) as min_unit_price,
        max(unitprice) as max_unit_price,
        avg(unitprice) as avg_unit_price,
        count(*)       as pricebook_entry_count
    from {{ ref('stg_salesforce__pricebook_entry') }}
    where isactive = true
      and isdeleted = false
    group by 1

),

final as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['p.product_id']) }} as product_sk,

        -- natural key
        p.product_id,

        -- attributes
        p.name                      as product_name,
        p.productcode               as product_code,
        p.stockkeepingunit          as sku,
        p.family                    as product_family,
        p.type                      as product_type,
        p.productclass              as product_class,
        p.quantityunitofmeasure     as unit_of_measure,
        p.description,

        -- flags
        p.isactive                  as is_active,
        p.isarchived                as is_archived,

        -- pricing from pricebook
        pe.min_unit_price,
        pe.max_unit_price,
        pe.avg_unit_price,
        pe.pricebook_entry_count,

        -- metadata
        p.createddate               as created_at,
        p.lastmodifieddate          as last_modified_at,

        -- derived fields
        case
            when pe.avg_unit_price >= 10000 then 'Premium'
            when pe.avg_unit_price >= 1000  then 'Standard'
            when pe.avg_unit_price > 0      then 'Budget'
            else 'Unpriced'
        end                         as price_range,

        case
            when pe.pricebook_entry_count > 0 then true
            else false
        end                         as has_active_pricing,

        case
            when pe.max_unit_price is not null
             and pe.min_unit_price is not null
             and pe.max_unit_price > 0
            then round(
                (pe.max_unit_price - pe.min_unit_price)
                / pe.max_unit_price * 100,
                2
            )
            else null
        end                         as price_variance_pct

    from products   p
    left join pricebook_entries pe on p.product_id = pe.product2id

)

select * from final