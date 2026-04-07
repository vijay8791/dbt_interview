

with source as (

    select * from {{ source('salesforce', 'case_history_2') }}

),

renamed as (

    select
        id as case_history_id,
        caseid,
        ownerid,
        status,
        previousupdate,
        lastmodifieddate,
        lastmodifiedbyid,
        isdeleted,
        systemmodstamp

    from source

)

select * from renamed
