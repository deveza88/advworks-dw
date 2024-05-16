WITH sales_data AS (
    SELECT
        sls.salesorderid AS salesorder_id,
        sls.orderqty,
        sls.salesorderdetailid AS salesorderdetail_id,
        sls.unitprice,
        sls.productid AS product_id
    from {{ source('sales', 'salesorderdetail') }} as sls
),
product_data AS (
    SELECT
        pd.productid AS product_id,
        pd.name AS product_name,
        pd.finishedgoodsflag,
        pd.productnumber AS product_number,
        pd.standardcost,
        pd.listprice,
        pd.daystomanufacture,
        pd.productline,
        pd.color,
        pd.sellstartdate AS sell_start
    from {{ source('production', 'product') }} as pd
)

SELECT 
    pd.product_id,
    sls.salesorderdetail_id,
    sls.salesorder_id,
    pd.product_name,
    pd.finishedgoodsflag,
    pd.product_number,
    pd.standardcost,
    pd.listprice,
    pd.listprice - pd.standardcost AS profit,
    pd.daystomanufacture,
    pd.color,
    pd.sell_start,
    sls.orderqty,
    sls.unitprice
FROM product_data pd
LEFT JOIN sales_data sls ON pd.product_id = sls.product_id