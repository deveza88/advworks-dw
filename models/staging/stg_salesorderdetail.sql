with source_data as (
    select
        salesorderid
        , orderqty
        , salesorderdetailid
        , unitprice
        , specialofferid
        , modifieddate
        , rowguid
        , productid as product_id
        , unitpricediscount
    from {{ source('sales', 'salesorderdetail') }}
)
select *
from source_data