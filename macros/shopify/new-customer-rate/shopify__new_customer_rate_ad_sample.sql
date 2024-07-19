{% macro shopify__new_customer_rate_ad_sample(
    sample_offer_name,
    campaigns,
    daily=False,
    start_date=None,
    end_date=None
) -%}

{% set orders_model='int_shopify__orders_non_recurring_' ~ sample_offer_name -%}

with 

orders as (

    select * from {{ ref(orders_model) }}

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
		
		case when tags ~* 'new customer' then
			1
		else
			0
		end as new_order,
		
		case when tags ~* 'returning customer' then
			1
		else
			0
		end as returning_order,
		
		total_price as total
		
	from
		orders
        
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
                nullif((sum(new_order) + sum(returning_order)), 0)::float
            )::numeric, 
            2
        ) as new_customer_rate

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