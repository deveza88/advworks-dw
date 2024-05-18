SELECT shipmethodid
    , name
    , shipbase
    , shiprate
    , rowguid
    , modifieddate
from {{ source("purchasing", "shipmethod") }}
