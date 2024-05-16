WITH dim_prod AS (
SELECT 
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
    FROM {{ ref('dim_products') }} 
)
select*
from dim_prod


with dimension_loc as (
    select
        sto.businessentity_id,
        sto.storename,
        cus.store_id
    from {{ ref('stg_store') }} as sto
    left join {{ ref('stg_customer') }} as cus on sto.businessentity_id = cus.store_id
)
select
    businessentity_id,
    storename,
    store_id
from dimension_loc



surrogate_keys as (
    select

    product_id,


    cb.customer_id,
    cb.person_id,
    cb.stateprovince_id,
    cb.country_id,
    cb.store_id
