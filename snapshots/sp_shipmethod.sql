{% snapshot sp_shipmethod %}

{{
    config(
        target_schema='snapshots',
        unique_key='shipmethod_id',
        strategy='check',
        check_cols=['name']
    )
}}

select
    *
from {{ ref('stg_shipmethod') }}

{% endsnapshot %}
