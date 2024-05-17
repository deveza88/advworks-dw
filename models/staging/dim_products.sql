with dim_products as (
    with sales_data as (
        select
            sls.productid as product_id,
            sls.salesorderid as salesorder_id,
            sls.orderqty,
            sls.salesorderdetailid as salesorderdetail_id,
            sls.unitprice
        from {{ source('sales', 'salesorderdetail') }} as sls
    ),

    product_data as (
        select
            pd.productid as product_id,
            pd.name as product_name,
            pd.finishedgoodsflag,
            pd.productnumber as product_number,
            pd.standardcost,
            pd.listprice,
            pd.daystomanufacture,
            pd.productline,
            pd.color,
            pd.sellstartdate as sell_start
        from {{ source('production', 'product') }} as pd
    )

    select
        pd.product_id,
        sls.salesorderdetail_id,
        sls.salesorder_id,
        pd.product_name,
        pd.finishedgoodsflag,
        pd.product_number,
        pd.standardcost,
        pd.listprice,
        pd.daystomanufacture,
        pd.sell_start as sell_start_date,
        sls.orderqty as order_qty,
        sls.unitprice,
        pd.listprice - pd.standardcost as profit
    from product_data as pd
    left join sales_data as sls on pd.product_id = sls.product_id
)

select
    product_id,
    salesorderdetail_id,
    salesorder_id,
    product_name,
    finishedgoodsflag,
    product_number,
    standardcost,
    listprice,
    profit,
    daystomanufacture,
    sell_start_date,
    order_qty,
    unitprice
from dim_products
