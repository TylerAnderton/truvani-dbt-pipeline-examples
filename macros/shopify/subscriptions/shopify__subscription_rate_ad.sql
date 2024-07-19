{% macro shopify__subscription_rate_ad(
    orders_model,
    campaigns,
    daily=False,
    start_date=None,
    end_date=None
) %}

-- MAP OLD AD/ADSET NAMES TO NEW
{% set ad_name_map={
    'Protein50 Welcome Kit':'P50 Welcome - PC1',
    'Vchai - Image 1 - 1/16/24':'Vchai - I1-C1',
    'Vchai - Image 2 - 1/16/24':'Vchai - I2-C1',
    'Vchai - Image 3 - 1/16/24':'Vchai - I3-C1',
    'Mel Video 11/24/21 - Melanie':'Mel Video 11/24/21 - Melanie',
    'New Black Shaker ASMR':'NEW ASMR - PC1',
    'Black Shaker Brownies 1':'Brownies I1 - PC1',
    'Black Shaker Brownies 2':'Brownies I2 - PC1',
    'Black Shaker Brownies 3':'Brownies I3 - PC1',
    'Shaker Starter Kit Ad 2 - Co':'SHAKER ASMR - PC2',
    'DCT New Shaker 7/25':'DCT New 7/25 - PC1',
    'Pink Shaker Welcome Kit':'Pink Welcome - PC1',
    'Pink Shaker Chocolate Cake 2':'Pink Cake - PC1',
    'Winning ABO Seasonal Pink Sh':'Winning Pink - PI1-PC1',
    'Winning ABO Seasonal Pink Sh':'Mel Pink - PI1-PC1',
    'Mel Video 11/24/21 - Melanie':'Mel Video 11/24/21 - Melanie',
    'Winning Pink Shaker Ad - Kay':'Kayla Pink - PI1-PC1'
} %}

{% set adset_name_map={
    'Broad - F - US - 25-65 - New Ye':'Broad - F - US - 25-65 - 1/16',
    'Broad - F - US - 25-65 - Season':'Broad - F - US - 25-65 - Season'
} %}

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
            {% for tag, campaign_name in campaigns.items() -%}
                when tags ~* 'utm:campaign:{{tag}}' then '{{campaign_name}}'    
            {% endfor -%}
			else (regexp_match(tags, 'utm:campaign:([^,]*)')) [1] -- 'No campaign attribution'
		end as campaign_name, -- manually enter facebook campaign names

        case
            {% for old_name, new_name in adset_name_map.items() -%}
                when (regexp_match(tags, 'utm:term:([^,]*)')) [1] = '{{old_name}}' then '{{new_name}}'
            {% endfor -%}
			else (regexp_match(tags, 'utm:term:([^,]*)')) [1]
		end as adset_name,

        case
            {% for old_name, new_name in ad_name_map.items() -%}
                when (regexp_match(tags, 'utm:content:([^,]*)')) [1] = '{{old_name}}' then '{{new_name}}'
            {% endfor -%}
			else (regexp_match(tags, 'utm:content:([^,]*)')) [1]
		end as ad_name,
		
		-- (regexp_match(tags, 'utm:term:([^,]*)')) [1] as adset_name,
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

{{ shopify__combine_sub_rate_adset_names(daily) }}

select *
from final_union
order by
    {% if daily -%}
        date_pst desc,
    {%- endif %}

  	campaign_name asc,
  	adset_name asc,
  	ad_name asc

----

-- final as (

--     select
--         {% if daily -%}
--             created_at_pt::date as date_pst,
--         {% endif -%}

--         round(
--             sum(total_price)::numeric,
--             2
--         ) as total_revenue,

--         round(
--             sum(total_price * subscription)::numeric, 2
--         ) as subscription_revenue,

--         round(
--             (
--                 sum(total_price)::numeric
--                 - 
--                 sum(total_price * subscription)::numeric
--             ),
--             2
--         ) as otp_revenue,

--         count(*) as order_count,

--         sum(subscription) as subscription_count,

--         count(*) - sum(subscription) as otp_count,
        
--         round(
--             (
--                 sum(subscription)
--                 /
--                 count(*)::float
--             )::numeric,
--             2
--         ) as subscription_rate

--     from orders

--     {% if start_date and end_date -%}
--        where created_at_pt::date between make_date({{start_date}}) and make_date({{end_date}}) 
--     {%- endif %}

--     {% if daily or campaign -%}
--         group by
--     {%- endif %}

--     {% if daily -%}
--         date_pst
--         {% if campaign -%}
--             ,
--         {%- endif %}
--     {%- endif %}

--     {% if campaign -%}
--         utm_campaign
--     {%- endif %}
     
-- )

-- select *
-- from final

-- {% if daily or campaign -%}
--     order by
-- {%- endif %}

-- {% if daily -%}
--     date_pst desc
--     {% if campaign -%}
--         ,
--     {%- endif %}
-- {%- endif %}

-- {% if campaign -%}
--     utm_campaign asc
-- {%- endif %}
    
{% endmacro %}