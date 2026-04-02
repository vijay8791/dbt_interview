{% docs opportunity_outcome %}
Derived field classifying each opportunity as Won, Lost or Open.
- Won: isclosed = true and iswon = true
- Lost: isclosed = true and iswon = false
- Open: isclosed = false
{% enddocs %}

{% docs deal_size_category %}
Derived field classifying opportunity size based on amount.
- Large: amount >= 100,000
- Medium: amount >= 10,000
- Small: amount > 0
- Unknown: amount is null or zero
{% enddocs %}

{% docs days_to_close %}
Number of days between the opportunity created date and the close date.
Negative values indicate the close date was set before the record was created.
{% enddocs %}

{% docs hours_to_resolve %}
Number of hours between case creation and closure.
Negative values indicate data quality issues in the source system
where closeddate was recorded before createddate.
These rows are flagged by the is_between test with severity warn.
{% enddocs %}

{% docs days_to_convert %}
Number of days between lead creation and conversion date.
NULL for leads that have not yet been converted.
{% enddocs %}

{% docs lead_lifecycle_stage %}
Derived field classifying each lead current lifecycle position.
- Converted: lead has been converted to account/contact/opportunity
- Deleted: lead record has been soft-deleted in Salesforce
- Active: lead is open and being worked
{% enddocs %}

{% docs dbt_loaded_at %}
UTC timestamp when this row was last loaded by dbt.
Generated via the generate_audit_columns macro.
Used as high-water mark in incremental models.
{% enddocs %}

{% docs dbt_model_name %}
Name of the dbt model that created or last updated this row.
Generated via the generate_audit_columns macro using the this.name Jinja variable.
{% enddocs %}

{% docs resolution_speed %}
Derived field classifying case resolution speed based on hours_to_resolve.
- Fast: resolved within 4 hours
- Normal: resolved within 24 hours
- Slow: took more than 24 hours
- Open: case not yet closed
{% enddocs %}

{% docs account_tier %}
Derived account tier based on annual revenue.
- Enterprise: annual revenue >= 1 billion
- Mid-Market: annual revenue >= 100 million
- SMB: annual revenue >= 10 million
- Other: below threshold or unknown
{% enddocs %}
