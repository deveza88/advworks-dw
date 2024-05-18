with dim_reasons as (
    select
        sh.salesorder_id,
        sr.salesreason_id,
        sr.reason_name,
        sr.reasontype,
        sh.last_update
    from {{ ref('stg_salesorderheadersalesreason') }} sh
    join {{ ref('stg_salesreason') }} sr on sh.salesreason_id = sr.salesreason_id
)

select
    salesorder_id,
    reason_name,
    reasontype
from dim_reasons
