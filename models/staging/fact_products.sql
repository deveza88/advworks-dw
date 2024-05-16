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
),
dim_sales AS (
    SELECT
        salesorder_id,
        customer_id,
        ship_address_id,
        bill_address_id
    FROM {{ ref('dim_sales') }}
),
dim_customer AS (
    SELECT
        customer_id,
        person_id
    FROM {{ ref('dim_customer') }}
),
dim_reason AS (
    SELECT
        salesorder_id,
        reason_name
    FROM {{ ref('dim_reasons') }}
)

SELECT
    ROW_NUMBER() OVER () AS fact_id,
    dp.product_id,
    ds.salesorder_id,
    dc.customer_id,
    ds.ship_address_id AS ship_to_customer_id,
    ds.bill_address_id AS address_of_customer_id,
    dp.salesorderdetail_id,
    dp.product_name,
    dp.finishedgoodsflag,
    dp.product_number,
    dp.standardcost,
    dp.listprice,
    dp.profit,
    dp.daystomanufacture,
    dp.color,
    dp.sell_start,
    dp.orderqty,
    dp.unitprice,
    dr.reason_name
FROM dim_prod dp
JOIN dim_sales ds ON dp.salesorder_id = ds.salesorder_id
JOIN dim_customer dc ON ds.customer_id = dc.customer_id
LEFT JOIN dim_reason dr ON ds.salesorder_id = dr.salesorder_id