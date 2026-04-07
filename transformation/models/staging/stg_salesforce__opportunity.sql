

with source as (

    select * from {{ source('salesforce', 'opportunity') }}

),

renamed as (

    select
        id as opportunity_id,
        isdeleted,
        accountid,
        isprivate,
        name,
        description,
        stagename,
        stagesortorder,
        amount,
        probability,
        expectedrevenue,
        totalopportunityquantity,
        closedate,
        type,
        nextstep,
        leadsource,
        isclosed,
        iswon,
        forecastcategory,
        forecastcategoryname,
        campaignid,
        hasopportunitylineitem,
        pricebook2id,
        ownerid,
        createddate,
        createdbyid,
        lastmodifieddate,
        lastmodifiedbyid,
        systemmodstamp,
        lastactivitydate,
        laststagechangedate,
        fiscalyear,
        fiscalquarter,
        contactid,
        primarypartneraccountid,
        contractid,
        lastamountchangedhistoryid,
        lastclosedatechangedhistoryid,
        deliveryinstallationstatus__c,
        trackingnumber__c,
        ordernumber__c,
        currentgenerators__c,
        maincompetitors__c

    from source

)

select * from renamed
