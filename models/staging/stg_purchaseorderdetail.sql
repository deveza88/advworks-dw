WITH stg_purchaseorderdetail AS (
    SELECT 
        pu.purchaseorderid,
        pu.purchaseorderdetailid,
        pu.duedate,
        pu.orderqty,
        pu.productid,
        pu.unitprice,
        pu.receivedqty,
        pu.rejectedqty,
        pu.modifieddate,
        pr.productid as product_2,
        pr.name
    FROM {{ source("purchasing", "purchaseorderdetail") }} pu
    LEFT JOIN {{ source("production", "product") }} pr 
    ON pu.productid = pr.productid
)
SELECT 
    purchaseorderid,
    purchaseorderdetailid,
    duedate,
    orderqty,
    unitprice,
    receivedqty,
    rejectedqty,
    modifieddate,
    productid,
    name
FROM stg_purchaseorderdetail

