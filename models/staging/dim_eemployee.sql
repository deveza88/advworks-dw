WITH dim_eemployee AS (
    SELECT
        dbt_scd_id,
        employee_id,
        CONCAT(first_name, ' ', last_name) AS full_name,
        dbt_valid_from,
        dbt_valid_to,
        dbt_updated_at,
        row_number() over(partition by employee_id order by dbt_valid_from) as row_nr
    FROM {{ ref('sp_pperson') }}
)

select
    dbt_scd_id as sk_employee_id,
    employee_id,
    full_name,
    case
        when row_nr = 1 then '1970-01-01'
        else dbt_valid_from
    end as valid_from,
    coalesce(dbt_valid_to, '2200-01-01') as valid_to,
    dbt_updated_at as last_updated_at
from dim_eemployee