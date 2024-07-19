{% macro join_shopify_and_cf_sample_revenue_orders_daily(shopify_rev_orders_model, cf_rev_orders_model) %}

with shopify as (

    select * 
    from {{ ref(shopify_rev_orders_model) }}

),

{% if cf_rev_orders_model %}
    
cf as (

    select * 
    from {{ ref(cf_rev_orders_model) }}

),

final as (

    select

        coalesce(
            shopify.date_pst,
            cf.date_pst
        ) as date_pst,

        coalesce(shopify.total, 0) 
        + coalesce(cf.revenue, 0) 
        as revenue,

        coalesce(shopify.order_count, 0) 
        + coalesce(cf.order_count, 0) 
        as order_count

    from shopify
    full join cf 
        using (date_pst)

)

{% else %}

final as (

    select
        coalesce(shopify.date_pst) as date_pst,
        coalesce(shopify.total, 0) as revenue,
        coalesce(shopify.order_count, 0) as order_count    
    from shopify

)

{% endif %}

select *
from final
order by
	date_pst desc
    
{% endmacro %}