{% macro shopify__reactivation_rate_sample(
    orders_model='stg_shopify__orders',
    tags=None,
    include_recurring=True,
    reactivation_interval='6 months',
    campaigns=None,
    start_date=None,
    end_date=None,
    daily=False
) %}
    
with

orders as (

    select 
        
        *

        {% if campaigns -%}
        ,
        case
            {% for ad_name, tag in campaigns.items() -%}
                when tags ~* 'utm:campaign:{{tag}}' then '{{ad_name}}'    
            {% endfor -%}
			else (regexp_match(tags, 'utm:campaign:([^,]*)')) [1]
		end as ad_name,

        (regexp_match(tags, 'utm:term:([^,]*)')) [1] as utm_term
		-- (regexp_match(tags, 'utm:content:([^,]*)')) [1] as ad_name
        {%- endif %}

    from {{ ref(orders_model) }}
    where
        cancelled_at is null

        {% if not include_recurring -%}
           and tags !~* 'subscription recurring order|active subscription' 
        {%- endif %}

        {% if start_date and end_date -%}
            and created_at_pt::date between make_date({{start_date}}) and make_date({{end_date}})    
        {%- endif %}

        {% if tags -%}
            and tags ~* '{{tags}}'
        {%- endif %}

),

total_order_count as (

    select 
        {% if daily -%}
           created_at_pt::date as date, 
        {%- endif %}

        {% if campaigns -%}
            ad_name,
            utm_term,
        {%- endif %}

        count(*) as total_order_count

    from orders

    {% if daily or campaigns -%}
    group by 
        {% if daily -%}
            date {% if campaigns -%},{%- endif %}
        {%- endif %}

        {% if campaigns -%}
            ad_name,
            utm_term
        {%- endif %}
        
    {%- endif %}

),

customers as (

    select distinct 

        {% if daily -%}
           created_at_pt, 
        {%- endif %}

        {% if campaigns -%}
            ad_name,
            utm_term,
        {%- endif %}

        email

    from orders

),

total_customer_count as (

    select 

        {% if daily -%}
           created_at_pt::date as date, 
        {%- endif %}

        {% if campaigns -%}
            ad_name,
            utm_term,
        {%- endif %}

        count(*) as total_customer_count

    from customers

    {% if daily or campaigns -%}
    group by 
        {% if daily -%}
            date {% if campaigns -%},{%- endif %}
        {%- endif %}

        {% if campaigns -%}
            ad_name,
            utm_term
        {%- endif %}
        
    {%- endif %}

),

returning_orders as (

    select *
    from orders
    where tags ~* 'returning'

),

returning_customers as (

    select distinct 

        {% if daily -%}
           created_at_pt, 
        {%- endif %}

        {% if campaigns -%}
            ad_name,
            utm_term,
        {%- endif %}

        email

    from returning_orders

),

recent_orders as (

    select 

        {% if daily -%}
            returning_orders.created_at_pt,
        {%- endif %}

        {% if campaigns -%}
            returning_orders.ad_name,
            returning_orders.utm_term,
        {%- endif %}

        returning_orders.name,
        returning_orders.email

    from returning_orders
    inner join {{ ref('stg_shopify__orders') }} as shopify_orders
        on returning_orders.email = shopify_orders.email
        and returning_orders.name != shopify_orders.name
        and shopify_orders.created_at_pt::date between (returning_orders.created_at_pt::date - interval '{{reactivation_interval}}') and returning_orders.created_at_pt::date

),

recent_order_count as (

    select 

        {% if daily -%}
           created_at_pt::date as date, 
        {%- endif %}

        {% if campaigns -%}
            ad_name,
            utm_term,
        {%- endif %}

        count(*) as recent_order_count

    from recent_orders

    {% if daily or campaigns -%}
    group by 
        {% if daily -%}
            date {% if campaigns -%},{%- endif %}
        {%- endif %}

        {% if campaigns -%}
            ad_name,
            utm_term
        {%- endif %}
        
    {%- endif %}

),

recent_customers as (

    select distinct 

        {% if daily -%}
           created_at_pt, 
        {%- endif %}

        {% if campaigns -%}
            ad_name,
            utm_term,
        {%- endif %}

        email

    from recent_orders

),

recent_customer_count as (

    select 

        {% if daily -%}
           created_at_pt::date as date, 
        {%- endif %}

        {% if campaigns -%}
            ad_name,
            utm_term,
        {%- endif %}

        count(*) as recent_customer_count

    from recent_customers
    
    {% if daily or campaigns -%}
    group by 
        {% if daily -%}
            date {% if campaigns -%},{%- endif %}
        {%- endif %}

        {% if campaigns -%}
            ad_name,
            utm_term
        {%- endif %}
        
    {%- endif %}

),

reactivated_customers as (

    select 

        {% if daily -%}
           created_at_pt, 
        {%- endif %}

        {% if campaigns -%}
            ad_name,
            utm_term,
        {%- endif %}

        email

    from returning_customers
    {% if daily -%}
        where (email, created_at_pt::date) not in (select email, created_at_pt::date from recent_customers)
    {% else -%}
        where email not in (select email from recent_customers)    
    {%- endif %}
    

),

reactivated_customer_count as (

    select 

        {% if daily -%}
           created_at_pt::date as date, 
        {%- endif %}

        {% if campaigns -%}
            ad_name,
            utm_term,
        {%- endif %}

        count(*) as reactivated_customer_count

    from reactivated_customers

    {% if daily or campaigns -%}
    group by 
        {% if daily -%}
            date {% if campaigns -%},{%- endif %}
        {%- endif %}

        {% if campaigns -%}
            ad_name,
            utm_term
        {%- endif %}
        
    {%- endif %}

),

joined_metrics as (

    select

        {% if daily -%}
            coalesce(
                total_order_count.date,
                total_customer_count.date,
                reactivated_customer_count.date
            ) as date, 
        {%- endif %}

        {% if campaigns -%}
            coalesce(
                total_order_count.ad_name,
                total_customer_count.ad_name,
                reactivated_customer_count.ad_name
            ) as ad_name,
            coalesce(
                total_order_count.utm_term,
                total_customer_count.utm_term,
                reactivated_customer_count.utm_term
            ) as utm_term,
        {%- endif %}

        coalesce(total_order_count.total_order_count, 0) as total_order_count,
        coalesce(total_customer_count.total_customer_count, 0) as total_customer_count,
        coalesce(reactivated_customer_count.reactivated_customer_count, 0) as reactivated_customer_count,

        round(
            (
                coalesce(reactivated_customer_count.reactivated_customer_count, 0)::float
                /
                nullif(total_customer_count.total_customer_count, 0)
            )::numeric,
            2
        ) as reactivation_rate

    from total_order_count

    {% if daily and campaigns -%}
        full join total_customer_count using(date, ad_name, utm_term)
        full join reactivated_customer_count using(date, ad_name, utm_term)
    {% elif daily -%}
        full join total_customer_count using(date)
        full join reactivated_customer_count using(date)
    {% elif campaigns -%}
        full join total_customer_count using(ad_name, utm_term)
        full join reactivated_customer_count using(ad_name, utm_term)
    {% else -%}
        full join total_customer_count on true
        full join reactivated_customer_count on true
    {%- endif %}
    
    {% if daily -%}
        order by date desc
    {%- endif %}

)

select *
from joined_metrics

{% endmacro %}