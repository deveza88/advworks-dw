with source_data as (
    select
        customerid as customer_id
        , personid as person_id
        , storeid as store_id
        , territoryid as territory_id
        , CURRENT_TIMESTAMP as last_update
    from {{ source('sales', 'customer') }}
)
select *
from source_data