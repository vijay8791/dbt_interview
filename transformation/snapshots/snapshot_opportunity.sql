{% snapshot snapshot_opportunity %}

{{
    config(
        target_schema = 'snapshots',
        unique_key    = 'opportunity_id',
        strategy      = 'check',
        check_cols    = [
            'stagename',
            'amount',
            'probability',
            'closedate',
            'forecastcategory',
            'isclosed',
            'iswon'
        ],
        tags = ['snapshots']
    )
}}

select
    opportunity_id,
    accountid,
    ownerid,
    campaignid,
    name                        as opportunity_name,
    stagename,
    amount,
    probability,
    expectedrevenue,
    closedate,
    forecastcategory,
    forecastcategoryname,
    isclosed,
    iswon,
    leadsource,
    type                        as opportunity_type,
    fiscalyear,
    fiscalquarter,
    createddate,
    lastmodifieddate

from {{ ref('stg_salesforce__opportunity') }}

{% endsnapshot %}