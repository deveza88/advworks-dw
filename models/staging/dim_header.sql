WITH dim_header AS (
    SELECT 
        purchaseorderid,
        duedate,
        SUM(orderqty) AS total_order_qty,
        SUM(receivedqty) AS total_qty_received,
        SUM(rejectedqty) AS total_qty_rejected,
        SUM(orderqty * unitprice) AS total_order_value
    FROM 
        {{ ref('stg_purchaseorderdetail') }}
    GROUP BY 
        purchaseorderid, duedate
)
SELECT *
FROM dim_header
ORDER BY purchaseorderid
