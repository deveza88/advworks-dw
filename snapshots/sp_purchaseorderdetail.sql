{% snapshot sp_purchaseorderdetail %}

{{
    config(
        target_schema='snapshots',
        unique_key='purchaseorder_id',
        strategy='check',
        check_cols=['purchaseorder_id','duedate','orderqty','unitprice','receivedqty','rejectedqty','modifieddate','name']
    )
}}

select
    *
from {{ ref('stg_purchaseorderdetail') }}

{% endsnapshot %}