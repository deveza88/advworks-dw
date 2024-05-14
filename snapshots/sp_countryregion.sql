{% snapshot sp_countryregion %}

{{
    config(
        target_schema='snapshots',
        unique_key='country_id',
        strategy='timestamp',
        updated_at='last_update'
    )
}}

select
    *
from {{ ref('stg_countryregion') }}

{% endsnapshot %}
