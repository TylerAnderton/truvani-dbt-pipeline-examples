{% macro shopify__new_customer_rate(
    orders_model,
    daily=False,
    campaign=False,
    start_date=None,
    end_date=None
) %}

with 

orders as (

    select * from {{ ref(orders_model) }}

),

orders_labeled as (

    select

		created_at_pt::date as date_pst,
		(regexp_match(tags, 'utm:campaign:([^,]*)')) [1] as utm_campaign,

		case 
            when tags ~* 'new customer' then 1
		    else 0
		end as new_order,

		case 
            when tags ~* 'returning customer' then 1
		    else 0
		end as returning_order,

		total_price as total

	from orders

),

final as (

    select
        {% if daily -%}
            date_pst,
        {% endif -%}

        {% if campaign -%}
            utm_campaign,
        {% endif -%}

        round(sum(total)::numeric, 2) as total_revenue,

        round(sum(total * new_order)::numeric, 2) as new_revenue,

        round(sum(total * returning_order)::numeric, 2) as returning_revenue,

        count(*) as total_orders,

        sum(new_order) as new_order_count,

        sum(returning_order) as returning_order_count,

        round(
            (
                sum(new_order) 
                / 
                nullif(
                    (sum(new_order) + sum(returning_order)),
                    0
                )::float
            )::numeric,
            2
        ) as new_customer_rate

    from orders_labeled 

    {% if start_date and end_date -%}
       where date_pst between make_date({{start_date}}) and make_date({{end_date}}) 
    {%- endif %}

    {% if daily or campaign -%}
        group by
    {%- endif %}

    {% if daily -%}
        date_pst
        {% if campaign -%}
            ,
        {%- endif %}
    {%- endif %}

    {% if campaign -%}
        utm_campaign
    {%- endif %}
     
)

select *
from final

{% if daily or campaign -%}
    order by
{%- endif %}

{% if daily -%}
    date_pst desc
    {% if campaign -%}
        ,
    {%- endif %}
{%- endif %}

{% if campaign -%}
    utm_campaign asc
{%- endif %}
    
{% endmacro %}