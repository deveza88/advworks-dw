{% snapshot sp_salesorderdetail %}

{{
    config(
        target_schema='snapshots',
        unique_key='salesorder_id',
        strategy='timestamp',
        updated_at='last_update'
    )
}}

select
    *
from {{ ref('stg_salesorderdetail') }}

{% endsnapshot %}