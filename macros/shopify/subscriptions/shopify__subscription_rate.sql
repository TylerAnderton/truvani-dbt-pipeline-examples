{% macro shopify__subscription_rate(
    orders_model,
    daily=False,
    campaign=False,
    start_date=None,
    end_date=None
) %}

with 

orders as (

    select 
        *,
		case when tags ~* 'subscription' then 1 else 0 end as subscription 
    from {{ ref(orders_model) }}

),

final as (

    select
        {% if daily -%}
            created_at_pt::date as date_pst,
        {% endif -%}

        {% if campaign -%}
            utm_campaign,
        {% endif -%}

        round(
            sum(total_price)::numeric,
            2
        ) as total_revenue,

        round(
            sum(total_price * subscription)::numeric, 2
        ) as subscription_revenue,

        round(
            (
                sum(total_price)::numeric
                - 
                sum(total_price * subscription)::numeric
            ),
            2
        ) as otp_revenue,

        count(*) as order_count,

        sum(subscription) as subscription_count,

        count(*) - sum(subscription) as otp_count,
        
        round(
            (
                sum(subscription)
                /
                count(*)::float
            )::numeric,
            2
        ) as subscription_rate

    from orders

    {% if start_date and end_date -%}
       where created_at_pt::date between make_date({{start_date}}) and make_date({{end_date}}) 
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