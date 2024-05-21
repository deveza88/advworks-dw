{% snapshot sp_dim_eemployee %}

{{
    config(
        target_schema='snapshots',
        unique_key='employee_id',
        strategy='check',
        check_cols=['full_name']
    )
}}

select
    *
from {{ ref('dim_eemployee') }}

{% endsnapshot %}
