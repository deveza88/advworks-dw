with
    source_data as (
        select
            salesorderid as salesorder_id,
            modifieddate as last_update,
            salesreasonid as salesreason_id
        from {{ source("sales", "salesorderheadersalesreason") }}
    )
select *
from source_data
