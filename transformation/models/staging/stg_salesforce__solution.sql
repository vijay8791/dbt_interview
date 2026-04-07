

with source as (

    select * from {{ source('salesforce', 'solution') }}

),

renamed as (

    select
        id as solution_id,
        isdeleted,
        solutionnumber,
        solutionname,
        ispublished,
        ispublishedinpublickb,
        status,
        isreviewed,
        solutionnote,
        caseid,
        ownerid,
        createddate,
        createdbyid,
        lastmodifieddate,
        lastmodifiedbyid,
        systemmodstamp,
        timesused,
        ishtml

    from source

)

select * from renamed
