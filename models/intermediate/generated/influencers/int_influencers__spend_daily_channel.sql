with 

date_series as (

    select
        generate_series(
            '2000-01-01'::date, 
            current_date, 
            '1 day'::interval
        )::date as report_date

),

generated_daily_spend as (

    select
    
        report_date,
        
        case 
	        when report_date <= '2020-01-01'::date then 100.00 -- sanitized spend amount and date
	        else 0.00
	    end as spend
    
    from date_series

)

select *
from generated_daily_spend
order by report_date desc