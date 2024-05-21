{% snapshot sp_productvendor %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=['standardprice']
    )
}}

select
    *
from {{ ref('stg_productvendor') }}

{% endsnapshot %}
