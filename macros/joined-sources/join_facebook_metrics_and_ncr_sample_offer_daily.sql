{% macro join_facebook_metrics_and_ncr_sample_offer_daily(
    facebook_metrics_model,
    ncr_model,
    subrate_model=None,
    reactivation_model=None,
    agg_level=None,
    agg_type='single'
) %}

{% set agg_layers='' %}

{% if agg_type|lower == 'multi' -%}
    
    {% if agg_level|lower == 'campaign' %}
        {% set agg_layers = agg_layers ~ '\ncampaign_name,' %}
        {% set unique_id='date_pst::text || campaign_name' %}
    {% endif %}
    {% if agg_level|lower == 'ad' %}
        {% set agg_layers = agg_layers ~ '\ncampaign_name,\nad_name,' %}
        {% set unique_id='date_pst::text || campaign_name || ad_name' %}
    {% endif %}

{% elif agg_type|lower == 'single' %}

    {% if agg_level|lower == 'campaign' %}
        {% set agg_layers = agg_layers ~ '\ncampaign_name,' %}
        {% set unique_id='date_pst::text || campaign_name' %}
    {% endif %}
    {% if agg_level|lower == 'ad' %}
        {% set agg_layers = agg_layers ~ '\nad_name,' %}
        {% set unique_id='date_pst::text || ad_name' %}
    {% endif %}

{%- endif %}


{{ config(
    materialized='incremental',
    unique_key='unique_id',
    on_schema_change='sync'
) }}

with facebook as (

    select * from {{ ref(facebook_metrics_model) }}

),

ncr as (

    select * from {{ ref(ncr_model) }}

),

{% if subrate_model -%}
subrate_daily as (

    select * from {{ ref(subrate_model) }}

),
{%- endif %}

{% if reactivation_model -%}
reactivation_daily as (

    select * from {{ ref(reactivation_model) }}

),


{%- endif %}
--

joined as (

    select
		coalesce(
            facebook.date_start,
            ncr.date_pst
            {% if subrate_model -%}
                ,
                subrate_daily.date_pst
            {%- endif %}
            {% if reactivation_model -%}
                ,
                reactivation_daily.date
            {%- endif %}
        ) as date_pst,
		
        coalesce(
            facebook.campaign_name,
            ncr.utm_term
            {% if subrate_model -%}
                ,
                subrate_daily.utm_term
            {%- endif %}
            {% if reactivation_model -%}
                ,
                reactivation_daily.utm_term
            {%- endif %},
            'OTHER'
        ) as campaign_name,

        coalesce(
            facebook.ad_name,
            ncr.ad_name
            {% if subrate_model -%}
                ,
                subrate_daily.ad_name
            {%- endif %}
            {% if reactivation_model -%}
                ,
                reactivation_daily.ad_name
            {%- endif %},
            'OTHER'
        ) as ad_name,
		
		coalesce(facebook.spend, 0) as spend,
		coalesce(facebook.unique_outbound_clicks, 0) as unique_outbound_clicks,
		coalesce(facebook.reach, 0) as reach,
		coalesce(facebook.impressions, 0) as impressions,
		
		coalesce(ncr.total_revenue, 0) as total_revenue,
		coalesce(ncr.new_revenue, 0) as new_revenue,
		coalesce(ncr.returning_revenue, 0) as returning_revenue,
		
		coalesce(ncr.total_orders, 0) as total_orders,
		coalesce(ncr.new_order_count, 0) as new_orders,
		coalesce(ncr.returning_order_count, 0) as returning_orders

        {% if subrate_model -%}
        ,
        coalesce(subrate_daily.subscription_revenue, 0) as subscription_revenue,
        coalesce(subrate_daily.otp_revenue, 0) as otp_revenue,

        coalesce(subrate_daily.subscription_count, 0) as subscription_orders,
        coalesce(subrate_daily.otp_count, 0) as otp_orders
        {%- endif %}

        {% if reactivation_model -%}
        ,
        coalesce(reactivation_daily.total_customer_count, 0) as total_customer_count,
        coalesce(reactivation_daily.reactivated_customer_count, 0) as reactivated_customer_count
        {%- endif %}
		
	from facebook
		
	full join ncr
		on 
			facebook.date_start = ncr.date_pst 
            and lower(facebook.campaign_name) = lower(ncr.utm_term) 
            and lower(facebook.ad_name) = lower(ncr.ad_name )

    {% if subrate_model -%}
    full join subrate_daily
		on 
			facebook.date_start = subrate_daily.date_pst 
            and lower(facebook.campaign_name) = lower(subrate_daily.utm_term) 
            and lower(facebook.ad_name) = lower(subrate_daily.ad_name)
    {%- endif %}

    {% if reactivation_model -%}
    full join reactivation_daily
		on 
			facebook.date_start = reactivation_daily.date
            and lower(facebook.campaign_name) = lower(reactivation_daily.utm_term) 
            and lower(facebook.ad_name) = lower(reactivation_daily.ad_name)
    {%- endif %}

),

initial_agg as (

	select

		date_pst,
		
        {{agg_layers}}

        sum(spend) as spend,
        sum(unique_outbound_clicks) as unique_outbound_clicks,
		sum(reach) as reach,
		sum(impressions) as impressions,

        round(
            (
                (
                    sum(spend) 
                    / 
                    nullif(sum(impressions), 0)
                )*1000
            )::numeric,
            2
        ) as cpm,

        round(
            (
                sum(spend) 
                / 
                nullif(sum(unique_outbound_clicks), 0)
            )::numeric,
            2
        ) as cpc,

	  	round(
            (
                sum(unique_outbound_clicks) 
                / 
                nullif(sum(reach), 0)::float
            )::numeric,
            4
        ) as ctr,

        sum(total_revenue) as total_revenue,
		sum(new_revenue) as new_revenue,
		sum(returning_revenue) as returning_revenue,
        {% if subrate_model -%}
            sum(subscription_revenue) as subscription_revenue,
        {%- endif %}
		
		sum(total_orders) as total_orders,
		sum(new_orders) as new_orders,
		sum(returning_orders) as returning_orders,
        {% if subrate_model -%}
            sum(subscription_orders) as subscription_orders,
        {%- endif %}
        {% if reactivation_model -%}
            sum(total_customer_count) as total_customer_count,
            sum(reactivated_customer_count) as reactivated_customer_count,
        {%- endif %}

        round(
            (
                sum(new_orders) 
                / 
                nullif((sum(new_orders) + sum(returning_orders)), 0)::float
            )::numeric, 
            2
        ) as new_customer_rate,

        {% if subrate_model -%}
        round(
            (
                sum(subscription_orders) 
                / 
                nullif((sum(total_orders)), 0)::float
            )::numeric, 
            2
        ) as subscription_rate,
        {%- endif %}

        {% if reactivation_model -%}
        round(
            (
                sum(reactivated_customer_count) 
                / 
                nullif((sum(total_customer_count)), 0)::float
            )::numeric, 
            2
        ) as reactivation_rate,
        {%- endif %}

        round(
            (
                sum(total_revenue) 
                / 
                nullif(
                    sum(spend),
                    0
                )
            )::numeric,
            2
        ) as roas,

		round(
            (
                sum(spend) 
                / 
                nullif(
                    sum(total_orders),
                    0
                )
            )::numeric,
            2
        ) as cpa,

		round(
            (
                sum(total_revenue) 
                / 
                nullif(
                    sum(total_orders),
                    0
                )
            )::numeric,
            2
        ) as aov,

        round(
            (
                sum(total_orders)::float
                /
                nullif(sum(unique_outbound_clicks), 0)
            )::numeric,
            2
        ) as conv_rate,

        round(
            (
                sum(total_revenue)
                /
                nullif(sum(unique_outbound_clicks), 0)
            )::numeric,
            2
        ) as rpv

    from joined

    group by
        date_pst
        {%- if (agg_layers != '') -%}
            ,
            {{agg_layers[:-1]}}
        {%- endif %}
		
)

select 
    {{unique_id}} as unique_id, -- unique_id for incremental updates
    *
from initial_agg
where date_pst is not null

order by 
	date_pst desc
    
    {% if agg_level and (agg_type|lower == 'multi') -%}
        ,
  	    campaign_name asc

        {% if (agg_level|lower == 'ad') -%}
            ,
  	        ad_name asc
        {%- endif %}

    {% elif (agg_type|lower == 'single') -%}

        {% if (agg_level|lower == 'campaign') -%}
                ,
            campaign_name asc

        {% elif (agg_level|lower == 'ad') -%}
            ,
  	        ad_name asc
        {% endif %}

    {%- endif %}
    
{% endmacro %}