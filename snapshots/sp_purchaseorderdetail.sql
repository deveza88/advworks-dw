{% snapshot sp_purchaseorderdetail %}

{{
    config(
        target_schema='snapshots',
        unique_key='purchaseorderid',
        strategy='check',
        check_cols=['purchaseorderid']
    )
}}

select
    *
from {{ ref('stg_purchaseorderdetail') }}

{% endsnapshot %}
