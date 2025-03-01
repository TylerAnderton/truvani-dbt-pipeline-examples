{% macro join_facebook_metrics_and_ncr_main_offer_daily(
    facebook_metrics_model,
    ncr_model,
    subrate_model=None,
    reactivation_model=None,
    agg_level=None,
    agg_type='single',
    fix_adsets=True
) %}

{% set agg_layers='' %}

{% if agg_type|lower == 'multi' -%}
    
    {% if agg_level|lower == 'campaign' %}
        {% set agg_layers = agg_layers ~ '\ncampaign_name,' %}
        {% set unique_id='date_pst::text || campaign_name' %}
    {% endif %}
    {% if agg_level|lower == 'adset' %}
        {% set agg_layers = agg_layers ~ '\ncampaign_name,\nadset_name,' %}
        {% set unique_id='date_pst::text || campaign_name || adset_name' %}
    {% endif %}
    {% if agg_level|lower == 'ad' %}
        {% set agg_layers = agg_layers ~ '\ncampaign_name,\nadset_name,\nad_name,' %}
        {% set unique_id='date_pst::text || campaign_name || adset_name || ad_name' %}
    {% endif %}

{% elif agg_type|lower == 'single' %}

    {% if agg_level|lower == 'campaign' %}
        {% set agg_layers = agg_layers ~ '\ncampaign_name,' %}
        {% set unique_id='date_pst::text || campaign_name' %}
    {% endif %}
    {% if agg_level|lower == 'adset' %}
        {% set agg_layers = agg_layers ~ '\nadset_name,' %}
        {% set unique_id='date_pst::text || adset_name' %}
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
            ncr.campaign_name
            {% if subrate_model -%}
                ,
                subrate_daily.campaign_name
            {%- endif %}
            {% if reactivation_model -%}
                ,
                reactivation_daily.campaign_name
            {%- endif %},
            'OTHER'
        ) as campaign_name,

        coalesce(
            facebook.adset_name,
            ncr.adset_name
            {% if subrate_model -%}
                ,
                subrate_daily.adset_name
            {%- endif %}
            {% if reactivation_model -%}
                ,
                reactivation_daily.adset_name
            {%- endif %},
            'OTHER'
        ) as adset_name,

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
            and facebook.campaign_name = ncr.campaign_name 
            and facebook.adset_name = ncr.adset_name 
            and facebook.ad_name = ncr.ad_name 

    {% if subrate_model -%}
    full join subrate_daily
		on 
			facebook.date_start = subrate_daily.date_pst 
            and facebook.campaign_name = subrate_daily.campaign_name 
            and facebook.adset_name = subrate_daily.adset_name 
            and facebook.ad_name = subrate_daily.ad_name
    {%- endif %}

    {% if reactivation_model -%}
    full join reactivation_daily
		on 
			facebook.date_start = reactivation_daily.date
            and facebook.campaign_name = reactivation_daily.campaign_name 
            and facebook.adset_name = reactivation_daily.adset_name 
            and facebook.ad_name = reactivation_daily.ad_name
    {%- endif %}

    -- full join ncr
    --     on coalesce(facebook.date_start, make_date(1900,01,01)) = coalesce(ncr.date_pst, make_date(1900,01,01))
    --         and coalesce(facebook.campaign_name, '') = coalesce(ncr.campaign_name, '')
    --         and coalesce(facebook.adset_name, '') = coalesce(ncr.adset_name, '')
    --         and coalesce(facebook.ad_name, '') = coalesce(ncr.ad_name, '')

    -- {% if subrate_model -%}
    -- full join subrate_daily
	-- 	on coalesce(facebook.date_start, make_date(1900,01,01)) = coalesce(subrate_daily.date_pst, make_date(1900,01,01))
    --         and coalesce(facebook.campaign_name, '') = coalesce(subrate_daily.campaign_name, '')
    --         and coalesce(facebook.adset_name, '') = coalesce(subrate_daily.adset_name, '')
    --         and coalesce(facebook.ad_name, '') = coalesce(subrate_daily.ad_name, '')
    -- {%- endif %}

    -- {% if reactivation_model -%}
    -- full join reactivation_daily
	-- 	on coalesce(facebook.date_start, make_date(1900,01,01)) = coalesce(reactivation_daily.date, make_date(1900,01,01))
    --         and coalesce(facebook.campaign_name, '') = coalesce(reactivation_daily.campaign_name, '')
    --         and coalesce(facebook.adset_name, '') = coalesce(reactivation_daily.adset_name, '')
    --         and coalesce(facebook.ad_name, '') = coalesce(reactivation_daily.ad_name, '')
    -- {%- endif %}

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
{%- if 
    fix_adsets
    and
    (
        ((agg_level|lower == 'adset') and (agg_type|lower == 'single'))
        or
        ((agg_level|lower == 'ad') and (agg_type|lower == 'multi'))
    )
-%}
,

matched_adsets as (

	select
	  	a.date_pst,

        {% if agg_level and (agg_type|lower == 'multi') -%}
            a.campaign_name,

            {% if (agg_level|lower == 'adset') or (agg_level|lower == 'ad') -%}
                case
                    when a.adset_name like '%' || b.adset_name || '%' then a.adset_name
                    else b.adset_name
                end as adset_name,
            {%- endif %}

            {% if (agg_level|lower == 'ad') -%}
                a.ad_name,
            {%- endif %}
        {% elif (agg_level|lower == 'adset') and (agg_type|lower == 'single') -%}
            case
                when a.adset_name like '%' || b.adset_name || '%' then a.adset_name
                else b.adset_name
            end as adset_name,
        {%- endif %}

	  	a.spend + b.spend as spend,
	  	a.unique_outbound_clicks + b.unique_outbound_clicks as unique_outbound_clicks,
	  	a.reach + b.reach as reach,
	  	a.impressions + b.impressions as impressions,
	  	
	  	round((((a.spend + b.spend) / nullif((a.impressions + b.impressions), 0))*1000)::numeric, 2) as cpm,
	  	round(((a.spend + b.spend) / nullif((a.unique_outbound_clicks + b.unique_outbound_clicks), 0))::numeric, 2) as cpc,
	  	round(((a.unique_outbound_clicks + b.unique_outbound_clicks) / nullif((a.reach + b.reach), 0)::float)::numeric, 4) as ctr,
	  	
	  	a.total_revenue + b.total_revenue as total_revenue,
	  	a.new_revenue + b.new_revenue as new_revenue,
	  	a.returning_revenue + b.returning_revenue as returning_revenue,
        {% if subrate_model -%}
            a.subscription_revenue + b.subscription_revenue as subscription_revenue,
        {%- endif %}
	  	
	  	a.total_orders + b.total_orders as total_orders,
	  	a.new_orders + b.new_orders as new_orders,
	  	a.returning_orders + b.returning_orders as returning_orders,
        {% if subrate_model -%}
            a.subscription_orders + b.subscription_orders as subscription_orders,
        {%- endif %}
        {% if reactivation_model -%}
            a.total_customer_count + b.total_customer_count as total_customer_count,
            a.reactivated_customer_count + b.reactivated_customer_count as reactivated_customer_count,
        {%- endif %}
	  	
	  	round(((a.new_orders + b.new_orders) 
	  	/ 
	  	nullif(
	  		((a.new_orders + b.new_orders) + (a.returning_orders + b.returning_orders))
	  		, 0)::float)::numeric, 2)
	  	as new_customer_rate,

        {% if subrate_model -%}
        round(
            (
                (a.subscription_orders + b.subscription_orders)
                / 
                nullif((a.total_orders + b.total_orders)::float, 0)
            )::numeric, 
            2
        ) as subscription_rate,
        {%- endif %}

        {% if reactivation_model -%}
        round(
            (
                (a.reactivated_customer_count + b.reactivated_customer_count)
                / 
                nullif((a.total_customer_count + b.total_customer_count)::float, 0)
            )::numeric, 
            2
        ) as reactivation_rate,
        {%- endif %}
	  	
	  	round(((a.total_revenue + b.total_revenue) / nullif((a.spend + b.spend), 0))::numeric, 2) as roas,
		round(((a.spend + b.spend) / nullif((a.total_orders + b.total_orders), 0))::numeric, 2) as cpa,
		round(((a.total_revenue + b.total_revenue) / nullif((a.total_orders + b.total_orders), 0))::numeric, 2) as aov,

        round(
            (
                (a.total_orders + b.total_orders)::float
                /
                nullif((a.unique_outbounjoin_facebook_metrics_and_ncr_sample_offer_daily_revenue)
                /
                nullif((a.unique_outbound_clicks + b.unique_outbound_clicks), 0)
            )::numeric,
            2
        ) as rpv
	  	
	from initial_agg a
	join initial_agg b
	on
	  	a.date_pst = b.date_pst

        {% if agg_level and (agg_type|lower == 'multi') -%}
            and a.campaign_name = b.campaign_name

            {% if (agg_level|lower == 'adset') or (agg_level|lower == 'ad') -%}
                and a.adset_name like '%' || b.adset_name || '%'
            {%- endif %}

            {% if (agg_level|lower == 'ad') -%}
                and a.ad_name = b.ad_name
            {%- endif %}
        {% elif (agg_level|lower == 'adset') and (agg_type|lower == 'single') -%}
            and a.adset_name like '%' || b.adset_name || '%'
        {%- endif %}
		
	where
	  	a.adset_name <> b.adset_name
),

final as (

    select 
        * 
    from 
        matched_adsets

    union

    select 
        *
    from 
        initial_agg
    where not exists (
        select 1
        from matched_adsets
        where 
            initial_agg.date_pst = matched_adsets.date_pst

            {% if agg_level and (agg_type|lower == 'multi') -%}
                and initial_agg.campaign_name = matched_adsets.campaign_name

                {% if (agg_level|lower == 'adset') or (agg_level|lower == 'ad') -%}
                    and (
                        matched_adsets.adset_name like '%' || initial_agg.adset_name || '%'
                        or initial_agg.adset_name like '%' || matched_adsets.adset_name || '%'
                    )
                {%- endif %}

                {% if (agg_level|lower == 'ad') -%}
                    and initial_agg.ad_name = matched_adsets.ad_name
                {%- endif %}
            {% elif (agg_level|lower == 'adset') and (agg_type|lower == 'single') -%}
                and (
                        matched_adsets.adset_name like '%' || initial_agg.adset_name || '%'
                        or initial_agg.adset_name like '%' || matched_adsets.adset_name || '%'
                    )
            {%- endif %}
    )
    and date_pst is not null

)
{%- endif %}

select 
    {{unique_id}} as unique_id, -- unique_id for incremental updates
    *

{%- if 
    fix_adsets
    and
    (
        ((agg_level|lower == 'adset') and (agg_type|lower == 'single'))
        or
        ((agg_level|lower == 'ad') and (agg_type|lower == 'multi'))
    )
-%}
    from final
{%- else -%}
    from initial_agg
{%- endif %}

where date_pst is not null

order by 
	date_pst desc
    
    {% if agg_level and (agg_type|lower == 'multi') -%}
        ,
  	    campaign_name asc

        {% if (agg_level|lower == 'adset') or (agg_level|lower == 'ad') -%}
            ,
  	        adset_name asc
        {%- endif %}

        {% if (agg_level|lower == 'ad') -%}
            ,
  	        ad_name asc
        {%- endif %}
    {% elif (agg_type|lower == 'single') -%}
        {% if (agg_level|lower == 'campaign') -%}
                ,
            campaign_name asc
        {% elif (agg_level|lower == 'adset') -%}
            ,
  	        adset_name asc
        {% elif (agg_level|lower == 'ad') -%}
            ,
  	        ad_name asc
        {% endif %}
    {%- endif %}
    
{% endmacro %}