with 

date_series as (

    select generate_series(
        '2023-05-18'::date, 
        current_date, 
        '1 day'::interval
    )::date as report_date

),

generated_daily_spend as (

    select

        report_date,
        
        case 
	        when report_date <= '2023-07-06'::date then 25.00
	        else 0.00
	    end as spend

    from date_series

)

select *
from generated_daily_spend
order by report_date desc