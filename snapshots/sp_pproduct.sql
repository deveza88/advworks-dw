{% snapshot sp_pproduct %}

{{
    config(
        target_schema='snapshots',
        unique_key='productid',
        strategy='timestamp',
        updated_at='last_update'
    )
}}

select
    *
from {{ ref('stg_pproduct') }}

{% endsnapshot %}