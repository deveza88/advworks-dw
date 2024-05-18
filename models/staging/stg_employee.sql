select
    e.businessentityid as employee_id,
    p.businessentityid,
    p.firstname as first_name,
    p.lastname as last_name
from {{ source("humanresources", "employee") }} e
left join {{ source("person", "person") }} p on e.businessentityid = p.businessentityid
