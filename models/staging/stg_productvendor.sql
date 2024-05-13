SELECT productid
    , businessentityid
    , averageleadtime
    , standardprice
    , lastreceiptcost
    , lastreceiptdate
    , minorderqty
    , maxorderqty
    , onorderqty
    , unitmeasurecode
    , modifieddate
from {{ source("purchasing", "productvendor") }}
