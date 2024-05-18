with dim_data as (
    select
        generate_series(
            (select min(duedate) from {{ ref('stg_purchaseorderdetail') }}),
            (select max(duedate) from {{ ref('stg_purchaseorderdetail') }}),
            interval '1 day'
        )::date as date
)

-- Final select statement
select
    date,
    extract(year from date) as year,
    extract(month from date) as month,
    extract(day from date) as day,
    to_char(date, 'YYYY-MM-DD') as date_string,
    to_char(date, 'YYYY-MM') as month_string,
    to_char(date, 'YYYY-Q') as quarter_string,
    to_char(date, 'YYYY') || '-' || to_char(date, 'IW') as week_string,
    case
        when extract(isodow from date) in (6, 7) then 'Weekend'
        else 'Weekday'
    end as day_type
from dim_data
