{% snapshot sp_productvendor %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='timestamp',
        updated_at='modifieddate'
    )
}}

select
    *
from {{ ref('stg_productvendor') }}

{% endsnapshot %}
