{% macro shopify__ncr_by_email(
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
        created_at_pt::date as date_pst,
		(regexp_match(tags, 'utm:campaign:([^,]*)')) [1] as utm_campaign
    from {{ ref(orders_model) }}
    {% if start_date and end_date -%}
       where created_at_pt::date between make_date({{start_date}}) and make_date({{end_date}}) 
    {%- endif %}

),

customers as (

    select *
    from {{ ref('stg_shopify__customers') }}

),

labeled_orders as (

    select 
        orders.*,
        customers.created_at_pt as customer_created_at_pt
    from orders
    inner join customers
        using (email)

),

new_orders as(

    select *
    from labeled_orders
    where created_at_pt::date = customer_created_at_pt::date

),

total_metrics as (

    select 
        {% if daily -%}
            date_pst,
        {% endif -%}

        {% if campaign -%}
            utm_campaign,
        {% endif -%}

        count(*) as total_orders,
        round(sum(total_price)::numeric, 2) as total_revenue

    from orders

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

),

total_labeled_metrics as (

    select 
        {% if daily -%}
            date_pst,
        {% endif -%}

        {% if campaign -%}
            utm_campaign,
        {% endif -%}

        count(*) as labeled_orders

    from labeled_orders

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

),

new_metrics as (

    select 
        {% if daily -%}
            date_pst,
        {% endif -%}

        {% if campaign -%}
            utm_campaign,
        {% endif -%}

        count(*) as new_orders,
        round(sum(total_price)::numeric, 2) as new_revenue
        
    from new_orders

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

),

final as (

    select
        {% if daily -%}
            total_metrics.date_pst,
        {% endif -%}

        {% if campaign -%}
            total_metrics.utm_campaign,
        {% endif -%}

        total_metrics.total_orders,
        total_metrics.total_revenue,
        new_metrics.new_orders,
        new_metrics.new_revenue,

        round((new_metrics.new_orders::float / total_labeled_metrics.labeled_orders)::numeric, 2) as new_customer_rate

    from total_metrics
    full join new_metrics
        {% if daily -%}
            using (date_pst
            {% if campaign -%}
                , utm_campaign)
            {% else -%}
                )
            {%- endif %}
        {% elif campaign -%}
            using (utm_campaign)
        {% else -%}
            on true
        {%- endif %}
        
    full join total_labeled_metrics
        {% if daily -%}
            using (date_pst
            {% if campaign -%}
                , utm_campaign)
            {% else -%}
                )
            {%- endif %}
        {% elif campaign -%}
            using (utm_campaign)
        {% else -%}
            on true
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