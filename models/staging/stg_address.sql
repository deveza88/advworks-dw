with source_data as (
    select
        addressid as address_id
        , stateprovinceid
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