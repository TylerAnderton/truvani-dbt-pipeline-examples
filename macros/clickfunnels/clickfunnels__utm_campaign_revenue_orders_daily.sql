{% macro clickfunnels__utm_campaign_revenue_orders_daily(orders_model) %}

with 

orders AS (

    select * from {{ref(orders_model)}}

),

final as (

    select
        date(created_at_pst) as date_pst,
        LOWER(utm_campaign) AS utm_campaign,
        sum(total) as revenue,
        count (unique_order) as order_count
    from orders
    group by 
        date_pst,
	    utm_campaign

)

select *
from final
order by 
    date_pst desc,
	utm_campaign asc
    
{% endmacro %}