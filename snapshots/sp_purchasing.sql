{% snapshot sp_purchasing %}

{{
    config(
        target_schema='snapshots',
        unique_key='purchaseorder_id',
        strategy='timestamp',
        updated_at='last_update'
    )
}}

select
    *
from {{ ref('stg_purchasing') }}

{% endsnapshot %}
