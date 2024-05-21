{% snapshot sp_pperson %}

{{
    config(
        target_schema='snapshots',
        unique_key='employee_id',
        strategy='check',
        check_cols=['first_name','last_name']
    )
}}

select
    *
from {{ ref('stg_pperson') }}

{% endsnapshot %}
