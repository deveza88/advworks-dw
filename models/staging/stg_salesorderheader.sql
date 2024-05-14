with source_data as (
    select
        salesorderid as salesorder_id 
        , shipmethodid 
        , billtoaddressid as bill_address_id
        , modifieddate
        , rowguid
        , taxamt
        , shiptoaddressid as ship_address_id
        , onlineorderflag
        , territoryid
        , status as order_status
        , orderdate
        , creditcardapprovalcode
        , subtotal
        , creditcardid
        , currencyrateid
        , revisionnumber
        , freight
        , duedate
        , totaldue
        , customerid
        , salespersonid
        , shipdate
        , accountnumber
    from {{ source('sales', 'salesorderheader') }}
)
select *
from source_data