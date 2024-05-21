{% snapshot sp_purchaseorderheader %}

{{
    config(
        target_schema='snapshots',
        unique_key='purchaseorderid',
        strategy='check',
        check_cols=['status','employeeid','vendorid','shipmethodid']
    )
}}

select
    *
from {{ ref('stg_purchaseorderheader') }}

{% endsnapshot %}
