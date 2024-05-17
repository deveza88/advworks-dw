with source_data as (
    select
        businessentityid as store_id
        , name as storename
        , salespersonid
        , modifieddate as last_update
    from {{ source('sales', 'store') }}
)
select *
from source_data