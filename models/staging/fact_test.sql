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
        listprice - standardcost AS profit,
        daystomanufacture,
        color,
        sell_start,
        orderqty,
        unitprice
    FROM {{ ref('dim_products') }}
)