WITH dim_products AS (
    SELECT
        pu.purchaseorderid,
        pu.productid,
        pr.name
    FROM 
        {{ ref('stg_purchaseorderdetail') }} pu
    LEFT JOIN 
        {{ ref('stg_product') }} pr ON pu.productid = pr.productid
)

SELECT 
    purchaseorderid,
    productid,
    name
FROM dim_products
ORDER BY purchaseorderid