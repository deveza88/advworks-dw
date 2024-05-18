WITH dim_products AS (
    SELECT
        pu.purchaseorderid,
        pu.productid,
        pr.name,
        pu.modifieddate
    FROM 
        {{ ref('stg_purchaseorderdetail') }} pu
    LEFT JOIN 
        {{ ref('stg_pproduct') }} pr ON pu.productid = pr.productid
)

SELECT 
    purchaseorderid,
    productid,
    name,
    modifieddate
FROM dim_products
ORDER BY purchaseorderid