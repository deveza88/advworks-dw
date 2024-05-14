with source_data as (
    select
        addressid as address_id
        , stateprovinceid as stateprovince_id
        , city
        , addressline2
        , modifieddate
        , rowguid
        , postalcode
        , spatiallocation
        , addressline1	
    from {{ source('person', 'address') }}
)
select *
from source_data