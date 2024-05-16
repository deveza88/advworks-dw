with dim_sales as (
    select
        -- Columns from stg_product
        p.product_id,
        --p.product_name,
        --p.safetystocklevel,
        --p.finishedgoodsflag,
        --p.class,
        --p.makeflag,
        --p.product_number,
        --p.reorderpoint,
        --p.last_update as product_last_update,
        --p.rowguid as product_row_guid,
        --p.productmodelid,
        --p.weightunitmeasurecode,
        --p.standardcost,
        --p.productsubcategoryid,
        --p.listprice,
        --p.daystomanufacture,
        --p.productline,
        --p.color,
        --p.sellstartdate,
        --p.product_weight,
        -- Columns from stg_salesorderdetail
        sod.salesorder_id,
        --sod.orderqty,
        sod.salesorderdetail_id,
        --sod.unitprice,
        --sod.specialofferid,
        -- Columns from stg_salesorderheader
        --soh.shipmethodid,
        soh.bill_address_id,
        --soh.taxamt,
        soh.ship_address_id,
        --soh.onlineorderflag,
        --soh.territoryid,
        --soh.order_status,
        soh.order_date,
       -- soh.creditcardapprovalcode,
        --soh.subtotal,
        soh.creditcard_id,
        --soh.currencyrateid,
        --soh.revisionnumber,
        --soh.freight,
        --soh.duedate,
        --soh.totaldue,
        soh.customer_id,
        --soh.salespersonid,
        soh.shipdate as shipdate
        --soh.accountnumber
    from {{ ref('stg_product') }} p
    left join {{ ref('stg_salesorderdetail') }} sod 
        on p.product_id = sod.product_id
    left join {{ ref('stg_salesorderheader') }} soh 
        on sod.salesorder_id = soh.salesorder_id
)
-- Final select statement
select *
from dim_sales