{% macro shopify__subscription_rate_ad_sample(
    orders_model,
    campaigns,
    daily=False,
    start_date=None,
    end_date=None
) %}

with 

orders as (

    select 
        *
    from {{ ref(orders_model) }}

),

orders_labeled as (

	select
        {% if daily -%}
            created_at_pt::date as date_pst,
        {% endif -%}
		
		case
            {% for ad_name, tag in campaigns.items() -%}
                when tags ~* 'utm:campaign:{{tag}}' then '{{ad_name}}'    
            {% endfor -%}
			else (regexp_match(tags, 'utm:campaign:([^,]*)')) [1]
		end as ad_name,

        (regexp_match(tags, 'utm:term:([^,]*)')) [1] as utm_term,
		-- (regexp_match(tags, 'utm:content:([^,]*)')) [1] as ad_name,
		
		total_price,

		case 
            when tags ~* 'subscription' then 1 
            else 0 
        end as subscription 
		
	from orders
        
    {% if start_date and end_date -%}
        where created_at_pt::date between make_date({{start_date}}) and make_date({{end_date}}) 
    {%- endif %}

), 

agg as (

	select
		{% if daily -%}
            date_pst,
        {% endif -%}

		ad_name,
		utm_term,

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

	from orders_labeled

	group by
		{% if daily -%}
            date_pst,
        {% endif -%}

		ad_name,
		utm_term

)

select *
from agg
order by
    {% if daily -%}
        date_pst desc,
    {%- endif %}

  	ad_name asc,
  	utm_term asc
    
{% endmacro %}