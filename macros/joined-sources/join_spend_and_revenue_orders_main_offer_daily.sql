{% macro join_spend_and_revenue_orders_main_offer_daily(
    spend_model,
    revenue_orders_model,
    sample=False
) %}

with spend as (

    select * from {{ ref(spend_model) }}

),

revenue_orders as (

    select * from {{ ref(revenue_orders_model) }}

),

{% if sample %}

final as (

    select 
        coalesce(
            spend.report_date,
            revenue_orders.date_pst
        ) as date_,
        
        round(
            coalesce(
                spend.spend,
                0
            )::numeric, 
            2
        ) as spend,
        
        coalesce(
            revenue_orders.revenue,
            0::numeric
        ) as revenue,
        
        coalesce(
            revenue_orders.order_count,
            0::bigint
        ) as order_count,
        
        round(
            (
                coalesce(
                    revenue_orders.revenue::numeric,
                    0::numeric
                ) 
                / 
                nullif(
                    coalesce(spend.spend, 0),
                    0
                )
            )::numeric,
            2
        ) as roas,
        
        round(
            (
                coalesce(
                    revenue_orders.revenue::numeric,
                    0::numeric
                ) 
                / 
                nullif(
                    coalesce(revenue_orders.order_count, 0),
                    0
                )
            )::numeric,
            2
        ) as aov,
        
        round(
            (
                coalesce(
                    spend.spend,
                    0::numeric
                ) 
                / 
                nullif(
                    coalesce(revenue_orders.order_count, 0),
                    0
                )
            )::numeric,
            2
        ) as cpa
        
    from 
        spend
    full join 
        revenue_orders 
        on spend.report_date = revenue_orders.date_pst
    
)
    
{% else -%}

final as (

    select 
        coalesce(
            spend.report_date,
            revenue_orders.date_pst
        ) as date_,
        
        round(
            coalesce(
                spend.spend,
                0
            )::numeric, 
            2
        ) as spend,
        
        coalesce(
            revenue_orders.standard_revenue,
            0::numeric
        ) as standard_revenue,
        
        coalesce(
            revenue_orders.standard_order_count,
            0::bigint
        ) as standard_order_count,
        
        coalesce(
            revenue_orders.popup_revenue,
            0::numeric
        ) as popup_revenue,
        
        coalesce(
            revenue_orders.popup_order_count,
            0::bigint
        ) as popup_order_count,
        
        coalesce(
            revenue_orders.total_revenue,
            0::numeric
        ) as total_revenue,
        
        coalesce(
            revenue_orders.total_order_count,
            0::bigint
        ) as total_order_count,
        
        round(
            (
                coalesce(
                    revenue_orders.total_revenue::numeric,
                    0::numeric
                ) 
                / 
                nullif(
                    coalesce(spend.spend, 0),
                    0
                )
            )::numeric,
            2
        ) as roas,
        
        round(
            (
                coalesce(
                    revenue_orders.total_revenue::numeric,
                    0::numeric
                ) 
                / 
                nullif(
                    coalesce(revenue_orders.total_order_count, 0),
                    0
                )
            )::numeric,
            2
        ) as aov,
        
        round(
            (
                coalesce(
                    spend.spend,
                    0::numeric
                ) 
                / 
                nullif(
                    coalesce(revenue_orders.total_order_count, 0),
                    0
                )
            )::numeric,
            2
        ) as cpa
        
    from 
        spend
    full join 
        revenue_orders 
        on spend.report_date = revenue_orders.date_pst
    
)

{%- endif %}

select *
from final
order by 
	date_ desc

{% endmacro %}