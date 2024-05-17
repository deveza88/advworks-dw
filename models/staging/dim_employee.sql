-- Joining stg_employee and stg_person tables
with dim_employee as (
select
    e.employee_id as employee_id,
    p.businessentity_id as person_id,
    concat(p.firstname, ' ', p.lastname) as full_name
from {{ ref('stg_employee') }} as e
left join {{ ref('stg_person') }} as p on e.employee_id = p.businessentity_id
)

-- Final select statement
select
    employee_id,
    person_id,
    full_name
from dim_employee
