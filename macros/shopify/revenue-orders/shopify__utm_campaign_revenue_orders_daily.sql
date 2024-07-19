{% macro shopify__utm_campaign_revenue_orders_daily(orders_model) %}

with 

orders as (

    select * 
    from {{ref(orders_model)}}

),

final as (

    select
        created_at_pt::date as date_pst,
        lower((regexp_match(tags, 'utm:campaign:([^,]*)')) [1]) as utm_campaign,
        -- lower((regexp_match(tags, 'utm:term:([^,]*)')) [1]) as utm_term,
        round(sum(total_price)::numeric, 2) as revenue,
        count(*) as order_count
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