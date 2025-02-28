with 

date_series as (

    select
        generate_series(
            '2023-04-20'::date, 
            current_date, 
            '1 day'::interval
        )::date as report_date

),

generated_daily_spend as (

    select
    
        report_date,
        
        case 
	        when report_date <= '2024-01-03'::date then 500.00
	        else 0.00
	    end as spend
    
    from date_series

)

select *
from generated_daily_spend
order by report_date desc