SELECT businessentityid
    , accountnumber
    , name
    , creditrating
    , preferredvendorstatus
    , activeflag
    , purchasingwebserviceurl
    , modifieddate
from {{ source("purchasing", "vendor") }}
