{% snapshot sp_dim_header %}

{{
    config(
        target_schema='snapshots',
        unique_key='purchaseorderid',
        strategy='timestamp',
        updated_at='duedate'
    )
}}

select
    *
from {{ ref('dim_header') }}

{% endsnapshot %}
