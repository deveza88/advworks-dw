SELECT purchaseorderid as purchaseorder_id
    , revisionnumber
    , status
    , employeeid as employee_id
    , vendorid
    , shipmethodid as shipmethod_id
    , orderdate
    , shipdate
    , subtotal
    , taxamt
    , freight
    , modifieddate
from {{ source("purchasing", "purchaseorderheader") }}
