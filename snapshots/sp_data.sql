{% snapshot sp_data %}

{{
    config(
        target_schema='snapshots',
        unique_key='date',
        strategy='timestamp',
        updated_at='date'
    )
}}

select
    *
from {{ ref('dim_data') }}

{% endsnapshot %}
