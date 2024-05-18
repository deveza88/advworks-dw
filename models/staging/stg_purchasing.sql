with source_data as (
    select
        pu.purchaseorderid as purchaseorder_id,
        pu.employeeid as employee_id,
        pu.vendorid as vendor_id,
        pu.shipmethodid as shipmethod_id,
        pu.orderdate as order_date,
        pu.shipdate,
        pu.subtotal,
        pu.modifieddate as last_update,
        pd.purchaseorderid as purchaseorderdetail_purchaseorderid,
        pd.purchaseorderdetailid,
        pd.duedate,
        pd.orderqty,
        pd.productid as purchaseorderdetail_productid,
        pd.unitprice,
        pd.receivedqty,
        pd.rejectedqty,
        sp.shipmethodid as shipmethod_shipmethodid,
        sp.name as shipmethod_name,
        pr.productid as product_productid,
        pr.name as product_name
    from {{ source("purchasing", "purchaseorderheader") }} as pu
    left join {{ source("purchasing", "purchaseorderdetail") }} as pd
        on pu.purchaseorderid = pd.purchaseorderid
    left join {{ source("production", "product") }} as pr
        on pd.productid = pr.productid
    left join {{ source("purchasing", "shipmethod") }} as sp
        on pu.shipmethodid = sp.shipmethodid
)
select
    purchaseorder_id,
    employee_id,
    vendor_id,
    shipmethod_id,
    order_date,
    shipdate as date_shipping,
    duedate as date_due,
    orderqty as order_qty,
    unitprice as unit_price,
    receivedqty as qty_received,
    rejectedqty as qty_rejected,
    shipmethod_name as name_shipping,
    product_productid as product_id,
    product_name as name_product,
    last_update
from source_data

