with source_data as (
    select
        businessentityid as businessentity_id,
        title,
        firstname,
        middlename,
        lastname,
        persontype,
        namestyle,
        suffix,
        modifieddate as last_update,
        rowguid,
        emailpromotion
    from {{ source('person', 'person') }}
)
select *
from source_data