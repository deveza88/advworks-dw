with source_data as (
    select
        creditcardid as creditcard_id
        , cardtype
        , expyear
        , modifieddate
        , expmonth
        , cardnumber
    from {{ source('sales', 'creditcard') }}
)
select *
from source_data