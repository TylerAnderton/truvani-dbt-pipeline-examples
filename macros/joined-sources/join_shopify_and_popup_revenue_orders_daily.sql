{% macro join_shopify_and_popup_revenue_orders_daily(shopify_rev_orders_model, popup_rev_orders_model) %}

with shopify as (

    select * from {{ ref(shopify_rev_orders_model) }}

),

{% if popup_rev_orders_model %}
    
popup as (

    select * from {{ ref(popup_rev_orders_model) }}

),

final as (

    select

        coalesce(
            shopify.date_pst,
            popup.date_pst
        ) as date_pst,

        coalesce(shopify.total, 0) as standard_revenue,
        coalesce(shopify.order_count, 0) as standard_order_count,
        coalesce(popup.revenue, 0) as popup_revenue,
        coalesce(popup.order_count, 0) as popup_order_count,

        coalesce(shopify.total, 0) 
        + coalesce(popup.revenue, 0) 
        as total_revenue,

        coalesce(shopify.order_count, 0) 
        + coalesce(popup.order_count, 0) 
        as total_order_count

    from shopify
    full join popup 
        using (date_pst)

)

{% else %}

final as (

    select
        coalesce(shopify.date_pst) as date_pst,
        coalesce(shopify.total, 0) as standard_revenue,
        coalesce(shopify.order_count, 0) as standard_order_count,
        0.00 as popup_revenue,
        0 as popup_order_count,
        coalesce(shopify.total, 0) as total_revenue,
        coalesce(shopify.order_count, 0) as total_order_count
    
    from shopify

)

{% endif %}

select *
from final
order by
	date_pst desc
    
{% endmacro %}