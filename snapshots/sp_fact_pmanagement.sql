{% snapshot sp_fact_pmanagement %}

{{
    config(
        target_schema='snapshots',
        unique_key='sk_product_id',
        strategy='timestamp',
        updated_at='last_update'
    )
}}

select
    *
from {{ ref('fact_pmanagement') }}

{% endsnapshot %}
