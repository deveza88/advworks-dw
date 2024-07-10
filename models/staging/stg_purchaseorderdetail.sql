WITH stg_purchaseorderdetail AS (
    SELECT
        pu.purchaseorderid AS purchaseorder_id,
        pu.purchaseorderdetailid AS purchaseorderdetail_id,
        pu.duedate,
        pu.orderqty,
        pu.productid AS product_id,
        pu.unitprice,
        pu.receivedqty,
        pu.rejectedqty,
        pu.modifieddate,
        pr.productid,
        pr.name
    FROM {{ source("purchasing", "purchaseorderdetail") }} AS pu
    LEFT JOIN {{ source("production", "product") }} AS pr
        ON pu.productid = pr.productid
)

SELECT
    purchaseorder_id,
    purchaseorderdetail_id,
    product_id,
    duedate,
    orderqty,
    unitprice,
    receivedqty,
    rejectedqty,
    modifieddate,
    name
FROM stg_purchaseorderdetail
ORDER BY product_id
