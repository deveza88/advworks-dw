{% snapshot sp_pproduct %}

{{
    config(
        target_schema='snapshots',
        unique_key='productid',
        strategy='check',
        check_cols=['name']
    )
}}

select
    *
from {{ ref('stg_pproduct') }}

{% endsnapshot %}