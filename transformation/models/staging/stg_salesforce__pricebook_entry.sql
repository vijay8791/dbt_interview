

with source as (

    select * from {{ source('salesforce', 'pricebook_entry') }}

),

renamed as (

    select
        id as pricebook_entry_id,
        pricebook2id,
        product2id,
        unitprice,
        isactive,
        usestandardprice,
        createddate,
        createdbyid,
        lastmodifieddate,
        lastmodifiedbyid,
        systemmodstamp,
        isdeleted,
        isarchived

    from source

)

select * from renamed
