select
    businessentityid as employee_id,
    firstname as first_name,
    lastname as last_name,
    modifieddate as last_update
from {{ source("person", "person") }} 