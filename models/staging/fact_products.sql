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