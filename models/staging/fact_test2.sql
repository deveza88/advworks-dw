with dimension as (
    select
        d.product_id,
        d.salesorder_id,
        d.salesorderdetail_id,
        d.bill_address_id,
        d.ship_address_id,
        d.creditcard_id,
        d.customer_id,
        d.ship_date,
        d.due_date,
        d.order_date,
        date_part('day', d.ship_date::timestamp - d.due_date::timestamp) as time_to_sell_days
    from {{ ref('dim_sales') }} d
    left join {{ ref('dim_data') }} dt on d.ship_date = dt.date
),

surrogate_keys as (
    select
        dsd.product_id as sk_product,
        dsd.salesorder_id as sk_salesorder,
        dsd.salesorderdetail_id as sk_salesorderdetail,
        dsd.bill_address_id as sk_adress_id,
        dsd.ship_address_id,
        dsd.creditcard_id,
        dsd.customer_id,
        dsd.ship_date as sk_ship_date,
        dsd.order_date,
        dr.reason_name as reason_name,
        dr.reasontype as reasontype,
        pr.product_name as product_name,
        pr.finishedgoodsflag,
        pr.product_number,
        pr.standardcost,
        pr.listprice,
        pr.profit,
        pr.daystomanufacture,
        pr.sell_start_date,
        pr.order_qty,
        pr.unitprice,
        cr.customer_id,
        cr.person_id,
        cr.stateprovince_id,
        cr.country_id,
        cr.store_id,
        e.employee_id as sk_employee,
        e.person_id,
        e.full_name as employee,
        st.store_id as sk_store,
        st.storename as store
    from dimension dsd
    left join {{ ref('dim_reasons') }} dr on dsd.salesorder_id = dr.salesorder_id
    left join {{ ref('dim_products') }} pr on dsd.product_id = pr.product_id
    left join {{ ref('dim_customer') }} cr on dsd.customer_id = cr.customer_id
    left join {{ ref('dim_employee') }} e on e.employee_id = cr.person_id
    left join {{ ref('stg_store') }} st on cr.store_id = st.store_id
),

final as (
    select
    sk_product,
    sk_salesorder,
    sk_salesorderdetail,
    sk_adress_id,
    sk_ship_date,
    sk_employee,
    reason_name,
    reasontype,
    product_name,
    employee,
    store
    from surrogate_keys
)

select
    *
from final