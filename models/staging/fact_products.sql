with
dim_prod as (
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
        color,
        sell_start,
        orderqty,
        unitprice
    from {{ ref("dim_products") }}
),

dim_sales as (
    select
        salesorder_id,
        customer_id,
        ship_address_id,
        bill_address_id
    from {{ ref("dim_sales") }}
),

dim_customer as (select
    customer_id,
    person_id
from {{ ref("dim_customer") }}),

dim_reason as (select
    salesorder_id,
    reason_name
from {{ ref("dim_reasons") }}),

surrogate_keys as (
    select
        dp.product_id as sk_product,
        ds.salesorder_id as sk_order_sale,
        dc.customer_id as sk_id_customer,
        ds.ship_address_id as sk_ship_to_customer_id,
        ds.bill_address_id as sk_address_of_customer_id,
        dp.salesorderdetail_id,
        dp.product_name,
        dp.finishedgoodsflag,
        dp.product_number,
        dp.standardcost as cost_product,
        dp.listprice as sell_price,
        dp.profit,
        dp.daystomanufacture,
        dp.color,
        dp.sell_start as data_sell_start,
        dp.orderqty as qty_order,
        dp.unitprice,
        dr.reason_name as reason_buy,
        row_number() over () as fact_id
    from dim_prod as dp
    inner join dim_sales as ds on dp.salesorder_id = ds.salesorder_id
    inner join dim_customer as dc on ds.customer_id = dc.customer_id
    left join dim_reason as dr on ds.salesorder_id = dr.salesorder_id
),

final as (
    select
        sk_product,
        sk_order_sale,
        sk_id_customer,
        sk_ship_to_customer_id,
        sk_address_of_customer_id,
        product_name,
        product_number,
        cost_product,
        sell_price,
        profit,
        daystomanufacture,
        data_sell_start,
        qty_order,
        reason_buy
    from surrogate_keys
)

select *
from
    final

    -- miss emplooyee and store
    -- reason to buy maybe dont have nulls