{% macro shopify__reactivation_rate(
    orders_model='stg_shopify__orders',
    tags=None,
    include_recurring=True,
    reactivation_interval='6 months',
    campaigns=None,
    start_date=None,
    end_date=None,
    daily=False
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

        {% if campaigns -%}
        ,
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
		end as ad_name
		
		-- (regexp_match(tags, 'utm:term:([^,]*)')) [1] as adset_name,
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
            campaign_name,
            adset_name,
            ad_name,
        {%- endif %}

        count(*) as total_order_count

    from orders

    {% if daily or campaigns -%}
    group by 
        {% if daily -%}
            date {% if campaigns -%},{%- endif %}
        {%- endif %}

        {% if campaigns -%}
            campaign_name,
            adset_name,
            ad_name
        {%- endif %}
        
    {%- endif %}

),

customers as (

    select distinct 

        {% if daily -%}
           created_at_pt, 
        {%- endif %}

        {% if campaigns -%}
            campaign_name,
            adset_name,
            ad_name,
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
            campaign_name,
            adset_name,
            ad_name,
        {%- endif %}

        count(*) as total_customer_count

    from customers

    {% if daily or campaigns -%}
    group by 
        {% if daily -%}
            date {% if campaigns -%},{%- endif %}
        {%- endif %}

        {% if campaigns -%}
            campaign_name,
            adset_name,
            ad_name
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
            campaign_name,
            adset_name,
            ad_name,
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
            returning_orders.campaign_name,
            returning_orders.adset_name,
            returning_orders.ad_name,
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
            campaign_name,
            adset_name,
            ad_name,
        {%- endif %}

        count(*) as recent_order_count

    from recent_orders

    {% if daily or campaigns -%}
    group by 
        {% if daily -%}
            date {% if campaigns -%},{%- endif %}
        {%- endif %}

        {% if campaigns -%}
            campaign_name,
            adset_name,
            ad_name
        {%- endif %}
        
    {%- endif %}

),

recent_customers as (

    select distinct 

        {% if daily -%}
           created_at_pt, 
        {%- endif %}

        {% if campaigns -%}
            campaign_name,
            adset_name,
            ad_name,
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
            campaign_name,
            adset_name,
            ad_name,
        {%- endif %}

        count(*) as recent_customer_count

    from recent_customers
    
    {% if daily or campaigns -%}
    group by 
        {% if daily -%}
            date {% if campaigns -%},{%- endif %}
        {%- endif %}

        {% if campaigns -%}
            campaign_name,
            adset_name,
            ad_name
        {%- endif %}
        
    {%- endif %}

),

reactivated_customers as (

    select 

        {% if daily -%}
           created_at_pt, 
        {%- endif %}

        {% if campaigns -%}
            campaign_name,
            adset_name,
            ad_name,
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
            campaign_name,
            adset_name,
            ad_name,
        {%- endif %}

        count(*) as reactivated_customer_count

    from reactivated_customers

    {% if daily or campaigns -%}
    group by 
        {% if daily -%}
            date {% if campaigns -%},{%- endif %}
        {%- endif %}

        {% if campaigns -%}
            campaign_name,
            adset_name,
            ad_name
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
                total_order_count.campaign_name,
                total_customer_count.campaign_name,
                reactivated_customer_count.campaign_name
            ) as campaign_name,
            coalesce(
                total_order_count.adset_name,
                total_customer_count.adset_name,
                reactivated_customer_count.adset_name
            ) as adset_name,
            coalesce(
                total_order_count.ad_name,
                total_customer_count.ad_name,
                reactivated_customer_count.ad_name
            ) as ad_name,
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
        full join total_customer_count using(date, campaign_name, adset_name, ad_name)
        full join reactivated_customer_count using(date, campaign_name, adset_name, ad_name)
    {% elif daily -%}
        full join total_customer_count using(date)
        full join reactivated_customer_count using(date)
    {% elif campaigns -%}
        full join total_customer_count using(campaign_name, adset_name, ad_name)
        full join reactivated_customer_count using(campaign_name, adset_name, ad_name)
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