{% snapshot sp_person %}

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
from {{ ref('stg_person') }}

{% endsnapshot %}