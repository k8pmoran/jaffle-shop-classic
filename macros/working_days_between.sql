{% macro working_days_between(start_date, end_date) %}
(
    select
        case
            when {{ start_date }} is null or {{ end_date }} is null then null
            else (
                select count(*) - 1
                from {{ ref('stg_date') }}
                where 
                    date_day between {{ start_date }} and {{ end_date }}
                    and day_name not in ('Sunday', 'Saturday')
            )
        end
)
{% endmacro %}