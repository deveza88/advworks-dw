{% snapshot sp_store %}

{{
    config(
        target_schema='snapshots',
        unique_key='businessentity_id',
        strategy='timestamp',
        updated_at='last_update'
    )
}}

select
    *
from {{ ref('stg_store') }}

{% endsnapshot %}