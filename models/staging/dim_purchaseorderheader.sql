WITH dim_purchaseorderheader AS (
SELECT 
    purchaseorderid,
    dbt_scd_id,
    employeeid,
    vendorid,
    shipmethodid,
    orderdate,
    shipdate,
    subtotal,
    taxamt,
    freight,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    row_number() over(partition by purchaseorderid order by dbt_valid_from) as row_nr
from {{ ref("sp_purchaseorderheader") }}
)

select
    dbt_scd_id as sk_purchaseorderid,
    purchaseorderid,
    employeeid,
    vendorid,
    shipmethodid,
    orderdate,
    shipdate,
    shipdate - orderdate as days_to_shipping,
    case
        when row_nr = 1 then '1970-01-01'
        else dbt_valid_from
    end as valid_from,
    coalesce(dbt_valid_to, '2200-01-01') as valid_to,
    dbt_updated_at as last_updated_at
from dim_purchaseorderheader