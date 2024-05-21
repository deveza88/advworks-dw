{% snapshot sp_fact_pmanagement %}

{{
    config(
        target_schema='snapshots',
        unique_key='sk_product_id',
        strategy='check',
        check_cols=['name_product','total_order_qty','total_order_value']
    )
}}

select
    *
from {{ ref('fact_pmanagement') }}

{% endsnapshot %}
