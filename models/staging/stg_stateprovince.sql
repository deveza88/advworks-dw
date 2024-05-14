with source_data as (
    select
        stateprovinceid as stateprovince_id
        , countryregioncode as country_id
        , modifieddate
        , rowguid
        , name as state_name
        , territoryid
        , isonlystateprovinceflag
        , stateprovincecode
    from {{ source('person', 'stateprovince') }}
)
select *
from source_data