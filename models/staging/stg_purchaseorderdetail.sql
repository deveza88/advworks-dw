SELECT 
    purchaseorderid
    , purchaseorderdetailid
    , duedate
    , orderqty
    , productid
    , unitprice
    , receivedqty
    , rejectedqty
    , modifieddate
from {{ source("purchasing", "purchaseorderdetail") }}
