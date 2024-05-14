with source_data as (
    select
        productid as product_id
        , name as product_name
        , safetystocklevel
        , finishedgoodsflag
        , class
        , makeflag
        , productnumber as product_number
        , reorderpoint
        , modifieddate
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
        , weight as product_weight
    from {{ source('production', 'product') }}
)
select *
from source_data