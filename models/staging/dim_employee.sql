-- Joining stg_employee and stg_person tables
select
    e.employee_id,
    p.businessentity_id as person_id,
    p.firstname,
    p.lastname,
from {{ ref('stg_employee') }} e
left join {{ ref('stg_person') }} p on e.employee_id = p.businessentity_id