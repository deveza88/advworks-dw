WITH dim_eemployee AS (
    SELECT
        employee_id,
        CONCAT(first_name, ' ', last_name) AS full_name,
        last_update
    FROM {{ ref('stg_pperson') }}
)
-- Final select statement
SELECT
    employee_id,
    full_name,
    last_update
FROM dim_eemployee
