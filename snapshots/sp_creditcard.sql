{% snapshot sp_creditcard %}

{{
    config(
        target_schema='snapshots',
        unique_key='creditcard_id',
        strategy='timestamp',
        updated_at='last_update'
    )
}}

select
    *
from {{ ref('stg_creditcard') }}

{% endsnapshot %}
