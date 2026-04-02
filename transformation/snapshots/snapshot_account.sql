{% snapshot snapshot_account %}

{{
    config(
        target_schema = 'snapshots',
        unique_key    = 'account_id',
        strategy      = 'check',
        check_cols    = [
            'name',
            'industry',
            'type',
            'rating',
            'annualrevenue',
            'numberofemployees',
            'sla__c',
            'active__c',
            'customerpriority__c'
        ],
        tags = ['snapshots']
    )
}}

select
    account_id,
    name,
    type,
    industry,
    rating,
    annualrevenue,
    numberofemployees,
    ownership,
    billingcity,
    billingstate,
    billingcountry,
    sla__c,
    slaexpirationdate__c,
    active__c,
    customerpriority__c,
    ownerid,
    createddate,
    lastmodifieddate

from {{ ref('stg_salesforce__account') }}

{% endsnapshot %}