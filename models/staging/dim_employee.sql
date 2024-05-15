-- Joining stg_employee and stg_person tables
select
    e.employee_id,
    p.businessentity_id,
    concat(p.firstname, ' ', p.lastname) as full_name
from {{ ref('stg_employee') }} e
left join {{ ref('stg_person') }} p on e.employee_id = p.businessentity_id