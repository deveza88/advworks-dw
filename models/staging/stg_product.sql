with source_data as (
    select
        productid
        , name
        , safetystocklevel
        , finishedgoodsflag
        , class
        , makeflag
        , productnumber
        , reorderpoint
        , modifieddate as last_update
        , rowguid
        , productmodelid
        , weightunitmeasurecode
        , standardcost
        , productsubcategoryid
        , listprice
        , daystomanufacture
        , productline
        , color
        , sellstartdate
        , weight
    from {{ source('production', 'product') }}
)
select *
from source_data