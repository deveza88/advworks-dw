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
),

surrogate_keys AS (
    SELECT
        ROW_NUMBER() OVER () AS fact_id,
        dp.product_id AS sk_product,
        ds.salesorder_id AS sk_order_sale,
        dc.customer_id AS sk_id_customer,
        ds.ship_address_id AS sk_ship_to_customer_id,
        ds.bill_address_id AS sk_address_of_customer_id,
        dp.salesorderdetail_id,
        dp.product_name AS product_name,
        dp.finishedgoodsflag,
        dp.product_number AS product_number,
        dp.standardcost AS cost_product,
        dp.listprice AS sell_price,
        dp.profit AS profit,
        dp.daystomanufacture,
        dp.color,
        dp.sell_start AS data_sell_start,
        dp.orderqty AS qty_order,
        dp.unitprice,
        dr.reason_name AS delivery_reason
    FROM dim_prod dp
    JOIN dim_sales ds ON dp.salesorder_id = ds.salesorder_id
    JOIN dim_customer dc ON ds.customer_id = dc.customer_id
    LEFT JOIN dim_reason dr ON ds.salesorder_id = dr.salesorder_id
),

final AS (
    SELECT
        sk_product,
        sk_order_sale,
        sk_id_customer,
        sk_ship_to_customer_id,
        sk_address_of_customer_id,
        product_name,
        product_number,
        cost_product,
        sell_price,
        profit,
        daystomanufacture,
        data_sell_start,
        qty_order,
        delivery_reason
    FROM surrogate_keys
)
SELECT
    *
FROM final
