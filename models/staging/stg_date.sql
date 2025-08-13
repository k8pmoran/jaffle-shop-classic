-- models/staging/stg_date_new.sql
{{ config(materialized='view') }}

with date_spine as (
    select
        cast(dateadd(day, value, '2020-01-01') as date) as date_day
    from generate_series(
        0, 
        datediff(day, '2020-01-01', getdate())
    )
)
select
    date_day,
    datepart(weekday, date_day) as day_of_week,
    format(date_day, 'dddd') as day_name,
    year(date_day) as year,
    month(date_day) as month,
    day(date_day) as day
from date_spine