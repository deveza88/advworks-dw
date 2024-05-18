{% snapshot sp_fact_pmanagement %}

{{
    config(
        target_schema='snapshots',
        unique_key='purchaseorder_id',
        strategy='timestamp',
        updated_at='due_date'
    )
}}

select
    *
from {{ ref('fact_pmanagement') }}

{% endsnapshot %}
