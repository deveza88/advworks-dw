SELECT purchaseorderid
    , revisionnumber
    , status
    , employeeid
    , vendorid
    , shipmethodid
    , orderdate
    , shipdate
    , subtotal
    , taxamt
    , freight
    , modifieddate
from {{ source("purchasing", "purchaseorderheader") }}
