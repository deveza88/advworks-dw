WITH dimension_ids AS (
    SELECT
        pu.sk_purchaseorder_id,
        pu.vendor_id,
        pu.shipmethod_id,
        pu.orderdate,
        pu.shipdate,
        em.sk_employee_id,
        pd.sk_products_id,
        pd.name AS name_product, 
        pd.safetystocklevel AS safetystocklevel,
        pu.shipdate - pu.orderdate AS days_to_shipping,
        he.purchaseorder_id AS purchaseorder_id,
        he.duedate AS due_date,
        he.duedate - pu.shipdate AS days_to_due,
        he.total_order_qty AS order_qty,
        he.total_qty_received AS qty_received,
        he.total_qty_rejected AS qty_rejected,
        he.total_order_value AS order_value
    FROM {{ ref('dim_purchaseorderheader') }} pu 
    LEFT JOIN {{ ref('dim_eemployee') }} em ON pu.employee_id = em.employee_id
        AND pu.orderdate BETWEEN em.valid_from AND em.valid_to
    LEFT JOIN {{ ref('dim_header') }} he ON pu.purchaseorder_id = he.purchaseorder_id
    LEFT JOIN {{ ref('dim_pproducts') }} pd ON pu.purchaseorder_id = pd.purchaseorder_id
        AND pu.orderdate BETWEEN pd.valid_from AND pd.valid_to
    LEFT JOIN {{ ref('sp_shipmethod') }} sp ON pu.shipmethod_id = sp.shipmethod_id
    LEFT JOIN {{ ref('dim_date') }} dt ON pu.orderdate = dt.date
    WHERE pu.orderdate BETWEEN pu.valid_from AND pu.valid_to

),

surrogate_keys AS (
    SELECT
        dids.sk_purchaseorder_id AS sk_purchaseorder_id,
        dids.sk_products_id AS sk_products_id,
        dids.sk_employee_id AS sk_employee_id,
        dids.shipmethod_id AS shipmethod_id,
        dids.vendor_id AS vendor_id,
        dids.orderdate AS order_date,
        dids.shipdate AS ship_date,
        dids.name_product,
        dids.days_to_shipping,
        dids.safetystocklevel,
        dids.purchaseorder_id,
        dids.due_date,
        dids.days_to_due,
        dids.order_qty,
        dids.qty_received,
        dids.qty_rejected,
        dids.order_value
    FROM dimension_ids dids
),

final_cte AS ( 
    SELECT
        sk_purchaseorder_id,
        sk_products_id,
        sk_employee_id,
        shipmethod_id,
        order_date,
        ship_date,
        name_product,
        days_to_shipping,
        safetystocklevel,
        due_date,
        days_to_due,
        order_qty,
        qty_received,
        qty_rejected,
        order_value
    FROM surrogate_keys
)

SELECT *
FROM final_cte