with 

generated_daily_spend as (

    select
    
        generate_series(
            '2023-01-01'::date, 
            current_date, 
            '1 day'::interval
        )::date as report_date,
        
        NULL::float as spend

)

select *
from generated_daily_spend
order by report_date desc