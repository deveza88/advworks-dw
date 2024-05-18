with qty_products as (
    select * 
    from {{ ref('dim_header') }}
),

surrogate_keys as (
    select purchaseorderid,
           employeeid,
           vendorid,
           shipmethodid,
           orderdate,
           shipdate,
           shipdate - orderdate AS days_to_shipping,
           modifieddate as last_update
    from {{ ref('stg_purchaseorderheader') }} pu
    left join {{ ref('dim_employee') }} em on pu.employeeid = em.employee_id
    left join {{ ref('dim_header') }} he on pu.purchaseorderid = he.purchaseorderid
    left join {{ ref('dim_products') }} pr on pu.productid = pr.productid  
    left join {{ ref('stg_shipmethod') }} sp on pu.shipmethodid = sp.shipmethodid  
    left join {{ ref('dim_data') }} dt on pu.last_update = dt.date
),

final as (
    select
           surrogate_keys.employeeid as sk_employeeid_id,
           surrogate_keys.vendorid as sk_vendorid_id,
           surrogate_keys.shipmethodid as sk_shipmethodid_id,
           surrogate_keys.orderdate as order_date,
           surrogate_keys.shipdate as ship_date,
           surrogate_keys.days_to_shipping,
           qty_products.purchaseorderid,
           qty_products.duedate as due_date,
           surrogate_keys.shipdate - qty_products.duedate AS days_to_due,
           qty_products.total_order_qty as order_qty,
           qty_products.total_qty_received as qty_received,
           qty_products.total_qty_rejected as qty_rejected,
           qty_products.total_order_value as order_value,
           surrogate_keys.last_update
    from surrogate_keys
    join qty_products on surrogate_keys.purchaseorderid = qty_products.purchaseorderid
)

select
    *
from final



