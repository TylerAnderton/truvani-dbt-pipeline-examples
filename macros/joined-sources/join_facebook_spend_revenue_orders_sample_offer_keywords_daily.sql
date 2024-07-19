{% macro join_facebook_spend_revenue_orders_sample_offer_keywords_daily(
    utm_campaign_reveneue_orders_model,
    ads_spend_model,
    keywords
) -%}

with 

utm_campaign_revenue_orders_daily as (

    select * from {{ ref(utm_campaign_reveneue_orders_model) }}

),

ads_spend_daily as (

    select * from {{ ref(ads_spend_model) }}

),

keyword_revenue_orders as (

	select 
		date_pst,

		case
			{% for keyword, campaign in keywords.items() -%}
                when utm_campaign = '{{campaign}}' then '{{keyword}}'
            {% endfor -%}
			else utm_campaign 
		end as keyword,

		revenue,
		order_count

	from utm_campaign_revenue_orders_daily

),

keyword_spend as (

	select 
		report_date,
		trim(regexp_replace(ad_name, '-.*', '')) as ad_name_keyword,
		round(sum(spend)::numeric, 2) as spend

	from ads_spend_daily

	group by
		report_date,
		ad_name_keyword

),

final as (

    select 

        coalesce(
            spend.report_date,
            revenue_orders.date_pst
        ) as report_date,
        
        coalesce(
            spend.ad_name_keyword,
            revenue_orders.keyword
        ) as keyword,
        
        spend.spend,
        revenue_orders.revenue,
        revenue_orders.order_count,
        
        round(
            (
                coalesce(revenue_orders.revenue, 0::numeric)::double precision 
                / 
                nullif(
                    coalesce(spend.spend, 0),
                    0
                )
            )::numeric,
            2
        ) as roas,
        
        round(
            (
                coalesce(revenue_orders.revenue, 0::numeric) 
                / 
                nullif(
                    coalesce(revenue_orders.order_count, 0),
                    0
                )
            )::numeric,
            2
        ) as aov,
        
        round(
            (
                coalesce(spend.spend, 0) 
                / 
                nullif(
                    coalesce(revenue_orders.order_count, 0),
                    0
                )
            )::numeric,
            2
        ) as cpa
        
    from keyword_spend as spend

    full join keyword_revenue_orders as revenue_orders
        on
            spend.report_date = revenue_orders.date_pst
            and lower(spend.ad_name_keyword) = lower(revenue_orders.keyword)

)

select *
from final
order by
	report_date desc,
	keyword asc
    
{% endmacro %}