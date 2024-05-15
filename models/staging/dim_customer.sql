-- Joining stg_customer, stg_person, stg_address, and other relevant tables
with customer_base as (
    select
        c.customer_id,
        p.businessentity_id as person_id,
        a.stateprovince_id,
        sp.country_id,
        s.customer_id as store_id
    from {{ ref('stg_customer') }} c
    left join {{ ref('stg_person') }} p on c.person_id = p.businessentity_id
    left join {{ ref('stg_salesorderheader') }} s on c.customer_id = s.customer_id
    left join {{ ref('stg_address') }} a on s.bill_address_id = a.address_id
    left join {{ ref('stg_stateprovince') }} sp on a.stateprovince_id = sp.stateprovince_id
)

-- Final select statement
select
    cb.customer_id,
    cb.person_id,
    cb.stateprovince_id,
    cb.country_id,
    cb.store_id
from customer_base cb