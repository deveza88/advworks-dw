{% snapshot sp_shipmethod %}

{{
    config(
        target_schema='snapshots',
        unique_key='shipmethodid',
        strategy='check',
        check_cols=['name']
    )
}}

select
    *
from {{ ref('stg_shipmethod') }}

{% endsnapshot %}
