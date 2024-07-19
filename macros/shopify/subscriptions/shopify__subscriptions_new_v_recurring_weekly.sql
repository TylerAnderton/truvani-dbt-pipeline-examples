{% macro shopify__subscriptions_new_v_recurring_weekly(
    start_date, 
    end_date
) %}

with 

orders as (

    select * from {{ ref('stg_shopify__orders') }}

),

subscriptions as (
	select  
		date_trunc ('week', created_at_pt::date)::date as date_week,
		total_price,
		tags
	from 
		orders
	where
		tags ~* 'subscription'
		and created_at_pt::date between make_date({{ start_date }}) and make_date({{ end_date }})
),

unioned as (

    select 
        'First Order' AS subscription_type,
        date_week,
        round(sum(total_price)::numeric, 2) as revenue,
        round(count(total_price)::numeric)::int as order_count
    from 
        subscriptions
    where
        tags ~* 'subscription first order|first subscription'
    group by 
        subscription_type,
        date_week
        
    union all 

    select 
        'Recurring' AS subscription_type,
        date_week,
        round(sum(total_price)::numeric, 2) as revenue,
        round(count(total_price)::numeric)::int as order_count
    from 
        subscriptions
    where
        tags ~* 'subscription recurring order|active subscription'
    group by 
        subscription_type,
        date_week
)

select *
from unioned
order by 
    date_week desc,
    subscription_type asc

{% endmacro %}