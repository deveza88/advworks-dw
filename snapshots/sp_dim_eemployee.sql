{% snapshot sp_dim_eemployee %}

{{
    config(
        target_schema='snapshots',
        unique_key='employee_id',
        strategy='timestamp',
        updated_at='last_update'
    )
}}

select
    *
from {{ ref('dim_eemployee') }}

{% endsnapshot %}
