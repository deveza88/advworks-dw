with source_data as (
    select
        salesorderid as salesorder_id 
        , modifieddate
        , salesreasonid
    from {{ source('sales', 'salesorderheadersalesreason') }}
)
select *
from source_data