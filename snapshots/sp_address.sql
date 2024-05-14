{% snapshot sp_address %}

{{
    config(
        target_schema='snapshots',
        unique_key='address_id',
        strategy='timestamp',
        updated_at='last_update'
    )
}}

select
    *
from {{ ref('stg_address') }}

{% endsnapshot %}
