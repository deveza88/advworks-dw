{% snapshot sp_stateprovince %}

{{
    config(
        target_schema='snapshots',
        unique_key='stateprovince_id',
        strategy='timestamp',
        updated_at='last_update'
    )
}}

select
    *
from {{ ref('stg_stateprovince') }}

{% endsnapshot %}