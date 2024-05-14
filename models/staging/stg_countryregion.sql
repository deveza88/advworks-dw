with source_data as (
    select
        countryregioncode as country_id
        , name as country_name
        , CURRENT_TIMESTAMP as last_update
    from {{ source('person', 'countryregion') }}
)
select *
from source_data