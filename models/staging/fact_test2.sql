WITH dim_prod AS (
SELECT 
    product_id,
    salesorderdetail_id,
    salesorder_id,
    product_name,
    finishedgoodsflag,
    product_number,
    standardcost,
    listprice,
    profit,
    daystomanufacture,
    color,
    sell_start,
    orderqty,
    unitprice
    FROM {{ ref('dim_products') }} 
)
select*
from dim_prod
