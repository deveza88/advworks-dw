with source_data as (
    select
        salesreasonid as salesreason_id
        , name as reason_name
        , reasontype
        , modifieddate as last_update
    from {{ source('sales', 'salesreason') }}
    where salesreasonid is not null -- Filtering out null values from salesreason_id
)
select *
from source_data