

with source as (

    select * from {{ source('salesforce', 'campaign') }}

),

renamed as (

    select
        id as campaign_id,
        isdeleted,
        name,
        parentid,
        type,
        status,
        startdate,
        enddate,
        expectedrevenue,
        budgetedcost,
        actualcost,
        expectedresponse,
        numbersent,
        isactive,
        description,
        numberofleads,
        numberofconvertedleads,
        numberofcontacts,
        numberofresponses,
        numberofopportunities,
        numberofwonopportunities,
        amountallopportunities,
        amountwonopportunities,
        hierarchynumberofleads,
        hierarchynumberofconvertedleads,
        hierarchynumberofcontacts,
        hierarchynumberofresponses,
        hierarchynumberofopportunities,
        hierarchynumberofwonopportunities,
        hierarchyamountallopportunities,
        hierarchyamountwonopportunities,
        hierarchynumbersent,
        hierarchyexpectedrevenue,
        hierarchybudgetedcost,
        hierarchyactualcost,
        ownerid,
        createddate,
        createdbyid,
        lastmodifieddate,
        lastmodifiedbyid,
        systemmodstamp,
        lastactivitydate,
        campaignmemberrecordtypeid

    from source

)

select * from renamed
