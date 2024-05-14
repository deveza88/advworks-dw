{% snapshot sp_salesreason %}

{{
    config(
        target_schema='snapshots',
        unique_key='salesreason_id',
        strategy='timestamp',
        updated_at='last_update'
    )
}}

select
    *
from {{ ref('stg_salesreason') }}

{% endsnapshot %}