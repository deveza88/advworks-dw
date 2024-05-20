with qty_products as (
    select *
    from {{ ref('dim_header') }}
),

surrogate_keys as (
    select
        pu.purchaseorderid,
        pu.employeeid,
        pu.vendorid,
        pu.shipmethodid,
        pu.orderdate,
        pu.shipdate,
        pu.modifieddate as last_update,
        pr.productid,
        pr.name as name_product, --add in 20/05/2024
        sp.name as name_ship,
        shipdate - orderdate as days_to_shipping
    from {{ ref('stg_purchaseorderheader') }} as pu
    left join {{ ref('dim_eemployee') }} as em on pu.employeeid = em.employee_id
    left join {{ ref('dim_header') }} as he on pu.purchaseorderid = he.purchaseorderid
    left join {{ ref('stg_purchaseorderdetail') }} as pd on pu.purchaseorderid = pd.purchaseorderid
    left join {{ ref('stg_pproduct') }} as pr on pd.productid = pr.productid
    left join {{ ref('stg_shipmethod') }} as sp on pu.shipmethodid = sp.shipmethodid
    left join {{ ref('dim_data') }} as dt on pu.modifieddate = dt.date
),

-- Subquery to calculate max qty_received for each shipmethod
max_qty_received as ( 
    select
        purchaseorderid,
        sum(total_qty_received) as max_qty_received
    from {{ ref('dim_header') }}
    group by purchaseorderid
), --add in 20/05/2024

final as (
    select
        surrogate_keys.purchaseorderid as sk_purchaseorder_id,
        surrogate_keys.productid as sk_product_id,
        surrogate_keys.employeeid as sk_employeeid_id,
        surrogate_keys.vendorid as sk_vendorid_id,
        surrogate_keys.name_ship as name_ship, --add in 20/05/2024
        surrogate_keys.orderdate as order_date,
        surrogate_keys.shipdate as ship_date,
        surrogate_keys.name_product,  -- Corrected alias reference
        surrogate_keys.days_to_shipping,
        qty_products.purchaseorderid as purchaseorder_id,
        qty_products.duedate as due_date,
        qty_products.duedate - surrogate_keys.shipdate AS days_to_due,
        qty_products.total_order_qty as order_qty,
        qty_products.total_qty_received as qty_received,
        qty_products.total_qty_rejected as qty_rejected,
        qty_products.total_order_value as order_value,
        surrogate_keys.last_update,
        max_qty_received.purchaseorderid,
        max_qty_received.max_qty_received --add in 20/05/2024
    from surrogate_keys
    inner join qty_products on surrogate_keys.purchaseorderid = qty_products.purchaseorderid
    left join max_qty_received on surrogate_keys.purchaseorderid = max_qty_received.purchaseorderid
        and surrogate_keys.purchaseorderid = max_qty_received.purchaseorderid
)

select
    sk_product_id,
    name_product,
    name_ship,
    count(distinct sk_purchaseorder_id) as total_orders,
    avg(days_to_shipping) as avg_days_to_shipping,
    sum(order_qty) as total_order_qty,
    sum(qty_received) as total_qty_received,
    max(max_qty_received) as max_qty_received, --add in 20/05/2024
    sum(qty_rejected) as total_qty_rejected,
    sum(order_value) as total_order_value,
    max(last_update) as last_update

from final
group by sk_product_id, name_ship, name_product
order by sk_product_id asc
