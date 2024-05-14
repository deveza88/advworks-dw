with source_data as (
    select
        salesorderid as salesorder_id
        , orderqty
        , salesorderdetailid as salesorderdetail_id
        , unitprice
        , specialofferid
        , modifieddate as last_update
        , rowguid
        , productid as product_id
        , unitpricediscount
    from {{ source('sales', 'salesorderdetail') }}
)
select *
from source_data