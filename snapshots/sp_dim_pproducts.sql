{% snapshot sp_dim_pproducts %}

{{
    config(
        target_schema='snapshots',
        unique_key='purchaseorderid',
        strategy='timestamp',
        updated_at='modifieddate'
    )
}}

select
    *
from {{ ref('dim_pproducts') }}

{% endsnapshot %}
