WITH dim_employee AS (
    SELECT
        employee_id,
        first_name,
        last_name
    FROM {{ ref('stg_employee') }}
)
-- Final select statement
SELECT
    employee_id,
    CONCAT(first_name, ' ', last_name) AS full_name  
FROM dim_employee