

with source as (

    select * from {{ source('salesforce', 'record_type') }}

),

renamed as (

    select
        id as record_type_id,
        name,
        modulenamespace,
        description,
        businessprocessid,
        sobjecttype,
        isactive,
        createdbyid,
        createddate,
        lastmodifiedbyid,
        lastmodifieddate,
        systemmodstamp,
        isdeleted

    from source

)

select * from renamed
