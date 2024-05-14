SELECT productid as product_id
    , businessentityid as businessentity_id
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
