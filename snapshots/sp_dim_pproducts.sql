{% snapshot sp_dim_pproducts %}

{{
    config(
        target_schema='snapshots',
        unique_key='purchaseorderid',
        strategy='check',
        check_cols=['name']
    )
}}

select
    *
from {{ ref('dim_pproducts') }}

{% endsnapshot %}
