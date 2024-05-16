with dim_prod as (
    product_id,
    salesorderdetail_id,
    salesorder_id,
    product_name,
    finishedgoodsflag,
    product_number,
    standardcost,
    listprice,
    profit,
    pdaystomanufacture,
    color,
    sell_start,
    orderqty,
    unitprice
)
select *
from {{ ref('dim_products') }} 