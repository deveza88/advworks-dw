{% snapshot sp_fact_pmanagement %}

{{
    config(
        target_schema='snapshots',
        unique_key='purchaseorder_id',
        strategy='timestamp',
        updated_at='duedate'
    )
}}

select
    *
from {{ ref('fact_pmanagement') }}

{% endsnapshot %}
