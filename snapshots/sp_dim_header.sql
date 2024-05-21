{% snapshot sp_dim_header %}

{{
    config(
        target_schema='snapshots',
        unique_key='purchaseorderid',
        strategy='check',
        check_cols=['duedate','total_order_qty', 'total_order_value']
    )
}}

select
    *
from {{ ref('dim_header') }}

{% endsnapshot %}
