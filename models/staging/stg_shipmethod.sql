SELECT shipmethodid as shipmethod_id
    , name
    , shipbase
    , shiprate
    , rowguid
    , modifieddate
from {{ source("purchasing", "shipmethod") }}
