{% macro join_spend_ncr_daily(
    spend_model,
    ncr_model
) %}

with

spend_daily as (

    select * from {{ ref(spend_model) }}

),

ncr_daily as (

    select * from {{ ref(ncr_model) }}

),

joined as (

    select
        coalesce(
            spend_daily.report_date,
            ncr_daily.date_pst
        ) as report_date,

        coalesce(spend_daily.spend, 0) as spend,

        coalesce(ncr_daily.total_revenue, 0) as total_revenue,
        coalesce(ncr_daily.new_revenue, 0) as new_revenue,
        coalesce(ncr_daily.returning_revenue, 0) as returning_revenue,

        coalesce(ncr_daily.total_orders, 0) as total_orders,
        coalesce(ncr_daily.new_order_count, 0) as new_order_count,
        coalesce(ncr_daily.returning_order_count, 0) as returning_order_count,

        ncr_daily.new_customer_rate
    
    from spend_daily
    
    full join ncr_daily
        on spend_daily.report_date = ncr_daily.date_pst
),

final as (

    select

        *,

        round(
            (
                coalesce(total_revenue,0) 
                / 
                nullif(
                    coalesce(spend, 0),
                    0
                )
            )::numeric,
            2
        ) as roas,
        
        round(
            (
                coalesce(total_revenue,0) 
                / 
                nullif(
                    coalesce(total_orders, 0),
                    0
                )
            )::numeric,
            2
        ) as aov,
        
        round(
            (
                coalesce(spend,0) 
                / 
                nullif(
                    coalesce(total_orders, 0),
                    0
                )
            )::numeric,
            2
        ) as cpa

    from joined

)

select * 
from final
order by report_date desc
    
{% endmacro %}