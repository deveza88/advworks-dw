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
    , modifieddate as last_update
from {{ source("purchasing", "productvendor") }}
