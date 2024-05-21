-- dim tem de ir snapshoot
-- alterar configuracoes do snapshot
-- adicionar status
-- select he.purchaseorderid, productid, orderqty, unitprice, receivedqty, rejectedqty, orderdate,shipdate,subtotal,taxamt, freight
--    from purchasing.purchaseorderheader as he
--	join purchasing.purchaseorderdetail hd on he.purchaseorderid = hd.purchaseorderid
-- sp shipment

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
        pr.productid,
        pr.name as name_product, 
        shipdate - orderdate as days_to_shipping
    from {{ ref('sp_purchaseorderheader') }} as pu
    left join {{ ref('dim_eemployee') }} as em on pu.employeeid = em.employee_id
    left join {{ ref('dim_header') }} as he on pu.purchaseorderid = he.purchaseorderid
    left join {{ ref('dim_pproducts') }} as pd on pu.purchaseorderid = pd.purchaseorderid
    left join {{ ref('sp_pproduct') }} as pr on pd.productid = pr.productid
    left join {{ ref('sp_shipmethod') }} as sp on pu.shipmethodid = sp.shipmethodid
    left join {{ ref('dim_data') }} as dt on pu.modifieddate = dt.date
),

final as (
    select
        surrogate_keys.purchaseorderid as sk_purchaseorder_id,
        surrogate_keys.productid as sk_product_id,
        surrogate_keys.employeeid as sk_employeeid_id,
        surrogate_keys.vendorid as sk_vendorid_id,
        surrogate_keys.shipmethodid as shipmethod_id,
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
        qty_products.total_order_value as order_value
    from surrogate_keys
    inner join qty_products on surrogate_keys.purchaseorderid = qty_products.purchaseorderid
)

select
    sk_product_id,
    name_product,
    count(distinct sk_purchaseorder_id) as total_orders,
    avg(extract(day from days_to_shipping)) as avg_days_to_shipping,
    max(date(ship_date)) as ship_date,
    max(date(order_date)) as order_date,
    sum(order_qty) as total_order_qty,
    sum(qty_received) as total_qty_received,
    sum(qty_rejected) as total_qty_rejected,
    sum(order_value) as total_order_value

from final
group by sk_product_id, name_product
order by sk_product_id asc
