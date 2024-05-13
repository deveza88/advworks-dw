with source_data as (
    select
        countryregioncode as country_id
        , modifieddate
        , name as country_name
    from {{ source('person', 'countryregion') }}
)
select *
from source_data