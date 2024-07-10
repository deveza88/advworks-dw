{% snapshot sp_purchaseorderheader %}

{{
    config(
        target_schema='snapshots',
        unique_key='purchaseorder_id',
        strategy='check',
        check_cols=['status','employee_id','vendorid','shipmethod_id']
    )
}}

select
    *
from {{ ref('stg_purchaseorderheader') }}

{% endsnapshot %}
