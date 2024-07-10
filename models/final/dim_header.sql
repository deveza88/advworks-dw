WITH dim_header AS (
    SELECT 
        purchaseorder_id,
        duedate,
        SUM(orderqty) AS total_order_qty,
        SUM(receivedqty) AS total_qty_received,
        SUM(rejectedqty) AS total_qty_rejected,
        SUM(receivedqty * unitprice) AS total_order_value
    FROM 
        {{ ref('sp_purchaseorderdetail') }}
    GROUP BY 
        purchaseorder_id, duedate
)
SELECT *
FROM dim_header
ORDER BY purchaseorder_id
