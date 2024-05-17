-- Joining stg_customer, stg_person, stg_address, and other relevant tables
with dim_customer as (
    select
        c.customer_id,
        p.businessentity_id as person_id,
        a.stateprovince_id,
        sp.country_id,
        c.store_id as store_id
    from {{ ref('stg_customer') }} as c
    left join {{ ref('stg_person') }} as p on c.person_id = p.businessentity_id
    left join {{ ref('stg_salesorderheader') }} as s on c.customer_id = s.customer_id
    left join {{ ref('stg_address') }} as a on s.bill_address_id = a.address_id
    left join {{ ref('stg_stateprovince') }} as sp on a.stateprovince_id = sp.stateprovince_id
)

-- Final select statement
select
    customer_id,
    person_id,
    stateprovince_id,
    country_id,
    store_id
from dim_customer
