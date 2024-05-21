WITH dim_products AS (
    SELECT
        pu.dbt_scd_id as dbt_scd_id,
        pu.purchaseorderid as purchaseorderid,
        pu.productid,
        pr.name,
        pu.dbt_valid_from as dbt_valid_from,
        pu.dbt_valid_to as dbt_valid_to,
        pu.dbt_updated_at as dbt_updated_at,
        row_number() over(partition by purchaseorderid order by pu.dbt_valid_from) as row_nr
    FROM 
        {{ ref('sp_purchaseorderdetail') }} pu
    LEFT JOIN 
        {{ ref('sp_pproduct') }} pr ON pu.productid = pr.productid
)

SELECT 
    dbt_scd_id as sk_purchaseorderid,
    purchaseorderid,
    productid,
    name,
    case
        when row_nr = 1 then '1970-01-01'
        else dbt_valid_from
    end as valid_from,
    coalesce(dbt_valid_to, '2200-01-01') as valid_to,
    dbt_updated_at as last_updated_at
FROM dim_products
ORDER BY productid ASC