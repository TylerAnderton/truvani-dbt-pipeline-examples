{% macro shopify__new_customer_rate_sample(
    sample_offer_name,
    shopify_orders_model=None,
    daily=false,
    utm_term=false,
    start_date=none,
    end_date=none
) -%}

{% set orders_model='int_clickfunnels__orders_' ~ sample_offer_name -%}

{% set sku_orders_model='int_shopify__orders_non_recurring_' ~ sample_offer_name ~ '_sku' -%}

with 

cf_orders as (

    select
        {% if utm_term -%}
           utm_term, 
        {%- endif %}

        unique_order,
        date(created_at_pst) as date_pst,
        email,
        total

    from {{ ref(orders_model) }}

),

{% if shopify_orders_model -%}
    
shopify_orders as (

    select 

        created_at_pt::date as date_pst,
        name,
        email,

        lower((regexp_match(tags, 'utm:term:([^,]*)')) [1]) as utm_term,
        
        case 
            when tags ~* 'new customer' then 1
		    else 0
		end as new_order,

		case 
            when tags ~* 'returning customer' then 1
		    else 0
		end as returning_order,

        total_price as total

    from {{ ref(shopify_orders_model) }}

),

{%- endif %}

sku_orders as (

    select 

        created_at_pt::date as date_pst,
        name,
        email,
        
        case 
            when tags ~* 'new customer' then 1
		    else 0
		end as new_order,

		case 
            when tags ~* 'returning customer' then 1
		    else 0
		end as returning_order,

        total_price as total

    from 
        {% if sample_offer_name == 'retail' -%}
            shopify_retail_orders
        {% else -%}
            {{ ref(sku_orders_model) }}
        {%- endif %}

    {% if shopify_orders_model -%}
       where name not in (select name from shopify_orders) 
    {%- endif %}

),

orders_labeled as (
	select 

        {% if utm_term -%}
           cf_orders.utm_term as utm_term, 
        {%- endif %}

		cf_orders.date_pst as date_pst,
		coalesce(cf_orders.unique_order, 0) as unique_order,

		coalesce(cf_orders.unique_order, 0) * sku_orders.new_order 
        as new_orders,

		coalesce(cf_orders.unique_order, 0) * sku_orders.returning_order 
        as returning_orders,
        
		cf_orders.total as total,
		cf_orders.total * sku_orders.new_order as new_revenue,
		cf_orders.total * sku_orders.returning_order as returning_revenue

	from cf_orders
	
	left join sku_orders 
        on lower(cf_orders.email) = lower(sku_orders.email)
        and cf_orders.date_pst = sku_orders.date_pst
),

final as (

    select
        {% if daily -%}
            date_pst,
        {% endif -%}

        {% if utm_term -%}
           utm_term, 
        {%- endif %}

        round(sum(total)::numeric, 2) as total_revenue,

        round(sum(total * new_orders)::numeric, 2) as new_revenue,

        round(sum(total * returning_orders)::numeric, 2) as returning_revenue,

        sum(unique_order) as total_orders,

        sum(new_orders) as new_order_count,

        sum(returning_orders) as returning_order_count,

        round(
            (
                sum(new_orders) 
                / 
                nullif(
                    (sum(new_orders) + sum(returning_orders)),
                    0
                )::float
            )::numeric,
            2
        ) as new_customer_rate

    from orders_labeled

    -- DATE RANGE
    {% if start_date and end_date -%}
       where date_pst between make_date({{start_date}}) and make_date({{end_date}}) 
    {%- endif %}

    -- GROUP BY
    {% if daily or utm_term -%}
        group by
    {%- endif %}

    {% if daily -%}
        date_pst
        {%- if utm_term %}
        , 
        {%- endif %}
    {%- endif %}

    {% if utm_term -%}
        utm_term
    {%- endif %}
     
)

{% if shopify_orders_model -%}

,

shopify_agg as (

    select
        {% if daily -%}
            date_pst,
        {%- endif %}

        {% if utm_term -%}
           utm_term, 
        {%- endif %}

        count(*) as total_orders,
        sum(new_order) as new_order_count,
        sum(returning_order) as returning_order_count,

        sum(total) as total_revenue,
        sum(total * new_order) as new_revenue,
        sum(total * returning_order) as returning_revenue

    from shopify_orders

    -- DATE RANGE
    {% if start_date and end_date -%}
       where date_pst between make_date({{start_date}}) and make_date({{end_date}}) 
    {%- endif %}
    
    -- GROUP BY
    {% if daily or utm_term -%}
        group by
    {%- endif %}

    {% if daily -%}
        date_pst
        {%- if utm_term %}
        , 
        {%- endif %}
    {%- endif %}

    {% if utm_term -%}
        utm_term
    {%- endif %}

),

all_final as (

    select

        {% if utm_term -%}
           coalesce(final.utm_term, shopify_agg.utm_term) as utm_term, 
        {%- endif %}

        {% if daily -%}
            coalesce(final.date_pst, shopify_agg.date_pst) as date_pst,
        {%- endif %}

        round((coalesce(final.total_revenue, 0) + coalesce(shopify_agg.total_revenue, 0))::numeric, 2) as total_revenue,
        round((coalesce(final.new_revenue, 0) + coalesce(shopify_agg.new_revenue, 0))::numeric, 2) as new_revenue,
        round((coalesce(final.returning_revenue, 0) + coalesce(shopify_agg.returning_revenue, 0))::numeric, 2) as returning_revenue,

        coalesce(final.total_orders, 0) + coalesce(shopify_agg.total_orders, 0) as total_orders,
        coalesce(final.new_order_count, 0) + coalesce(shopify_agg.new_order_count, 0) as new_order_count,
        coalesce(final.returning_order_count, 0) + coalesce(shopify_agg.returning_order_count, 0) as returning_order_count,

        round(
            (
                (coalesce(final.new_order_count, 0) + coalesce(shopify_agg.new_order_count, 0))
                / 
                nullif(
                    (
                        (coalesce(final.new_order_count, 0) + coalesce(shopify_agg.new_order_count, 0)) 
                        + 
                        (coalesce(final.returning_order_count, 0) + coalesce(shopify_agg.returning_order_count, 0))
                    ),
                    0
                )::float
            )::numeric,
            2
        ) as new_customer_rate

    from final
    full join shopify_agg 

    -- JOIN CONDITION
    {% if daily or utm_term -%}
        on
    {% else %}
        on true
    {%- endif %}

    {% if daily -%}
        final.date_pst = shopify_agg.date_pst
        {%- if utm_term %}
        and 
        {%- endif %}
    {%- endif %}

    {% if utm_term -%}
        final.utm_term = shopify_agg.utm_term
    {%- endif %}

)

{%- endif %}

select *
from {% if shopify_orders_model -%} all_final {% else %} final {%- endif %}

{% if daily or utm_term -%}
    order by
{%- endif %}

{% if daily -%}
    date_pst desc
    {%- if utm_term %}
       , 
    {%- endif %}
{%- endif %}

{% if utm_term -%}
    utm_term asc
{%- endif %}
    
{% endmacro %}