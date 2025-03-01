{% macro agg_kpi_monthly(
    kpi_daily_model,
    month_to_date=False
) %}

{{ config(
    materialized='incremental',
    unique_key='date_month',
    on_schema_change='sync'
) }}

with

kpi_daily as (

    select * from {{ ref(kpi_daily_model) }}

),

monthly_agg as (

	select 
		date_trunc('month', report_date)::date as date_month,
	
		round(sum(spend)::numeric, 2) as spend,

		round(sum(total_revenue)::numeric, 2) as total_revenue,		
		round(sum(new_revenue)::numeric, 2) as new_revenue,	
		round(sum(returning_revenue)::numeric, 2) as returning_revenue,
		
		sum(total_orders)::integer as total_orders,		
		sum(new_order_count)::integer as new_order_count,		
		sum(returning_order_count)::integer as returning_order_count
		
	from kpi_daily
		
    {% if month_to_date -%}
        where extract(day from report_date) <= extract(day from (current_date - interval '1 day'))
    {%- endif %}
		
	group by date_month

),

final as (

    select 
        *,

        round(
            (
                total_revenue 
                / 
                nullif(spend, 0)
            )::numeric, 
            2
        ) as roas,

        round(
            (
                total_revenue 
                / 
                nullif(total_orders, 0)
            )::numeric, 
            2
        ) as aov,

        round(
            (
                spend 
                / 
                nullif(total_orders, 0)
            )::numeric, 
            2
        ) as cpa,

        round(
            (
                new_order_count::float 
                / 
                nullif(new_order_count + returning_order_count, 0)
            )::numeric, 
            2
        ) as new_customer_rate

    from monthly_agg

)

select *
from final
order by date_month desc
    
{% endmacro %}