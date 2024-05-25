{% snapshot sp_purchaseorderdetail %}

{{
    config(
        target_schema='snapshots',
        unique_key='purchaseorderid',
        strategy='check',
        check_cols=['purchaseorderid','duedate','orderqty','unitprice','receivedqty','rejectedqty','modifieddate','name']
    )
}}

select
    *
from {{ ref('stg_purchaseorderdetail') }}

{% endsnapshot %}