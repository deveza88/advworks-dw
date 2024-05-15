with source_data as (
    select
        salesorderid as salesorder_id 
        , shipmethodid 
        , billtoaddressid as bill_address_id
        , modifieddate as last_update
        , rowguid
        , taxamt
        , shiptoaddressid as ship_address_id
        , onlineorderflag
        , territoryid
        , status as order_status
        , orderdate as order_date
        , creditcardapprovalcode
        , subtotal
        , creditcardid as creditcard_id
        , currencyrateid
        , revisionnumber
        , freight
        , duedate
        , totaldue
        , customerid as customer_id
        , salespersonid
        , shipdate
        , accountnumber
    from {{ source('sales', 'salesorderheader') }}
)
select *
from source_data