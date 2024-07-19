{% macro facebook__metrics(
    attn_truvani=None,
    campaign_name_includes=[],
    campaign_name_excludes=[],
    daily=False,
    agg_level=None,
    start_date=None,
    end_date=None
) %}

{% set regex_include=misc__join_regex_terms(campaign_name_includes) %}
{% set regex_exclude=misc__join_regex_terms(campaign_name_excludes) %}

{% set adset_trunc="regexp_replace(trim(left(adset_name, 31)), ',', ' ')" %}
{% set ad_trunc="regexp_replace(trim(left(ad_name, 28)), ',', ' ')" %}

{% set agg_layers='' %}

{% if agg_level|lower == 'campaign' %}
    {% set agg_layers = agg_layers ~ '\ncampaign_name,' %}
{% endif %}
{% if agg_level|lower == 'adset' %}
    {% set agg_layers = agg_layers ~ '\ncampaign_name,\nadset_name,' %}
{% endif %}
{% if agg_level|lower == 'ad' %}
    {% set agg_layers = agg_layers ~ '\ncampaign_name,\nadset_name,\nad_name,' %}
{% endif %}

with facebook_ads_insights as (

    select * from {{ ref('stg_facebook_main__ads_insights') }}

),

ads as (

	select 
		date_start,

		campaign_name,
		{{adset_trunc}} AS adset_name,
		{{ad_trunc}} AS ad_name,

		round(sum(spend)::numeric, 2) as spend,
		-- round(max(cpm)::numeric, 2) as cpm,

		sum(
            (unique_outbound_clicks_arr.item_object ->> 'value')::float
        )::int as unique_outbound_clicks,

		sum(reach) as reach,
		sum(impressions) as impressions,

		round(
            (
                max(spend) 
                / 
                nullif(
                    sum((unique_outbound_clicks_arr.item_object ->> 'value')::float),
                    0
                )
            )::numeric,
            2
        ) as cpc,

		round(
            (
                sum((unique_outbound_clicks_arr.item_object ->> 'value')::float) 
                / 
                nullif(
                    min(reach),
                    0
                )
            )::numeric,
            4
        ) as ctr

	from 
		facebook_ads_insights,
		jsonb_array_elements(unique_outbound_clicks) 
            with ordinality unique_outbound_clicks_arr(item_object, position)

	where
		spend <> 0
		
        {% if attn_truvani -%}
            
        and campaign_name ~* '{{attn_truvani}}'

        {%- endif %}

        {% if regex_include|length > 0 -%}

        and campaign_name ~* '{{regex_include}}'

        {%- endif %}

        {%- if regex_exclude|length > 0 -%}

        and campaign_name !~* '{{regex_exclude}}'

        {%- endif %}

        {% if start_date and end_date -%}
            and date_start between make_date({{start_date}}) and make_date({{end_date}}) 
        {%- endif %}
		
    group by
        date_start,
        campaign_name,
		{{adset_trunc}},
		{{ad_trunc}}

{#     {%- if daily -%}
        ,
        date_start
    {%- endif %} #}

),

final as (

	select 
		{% if daily -%}
            date_start,
        {% endif -%}

		{{agg_layers}}
		
		round(sum(spend)::numeric, 2) as spend,
		
		round(((sum(spend) / nullif(sum(impressions), 0))*1000)::numeric, 2) as cpm,
		
		sum(unique_outbound_clicks)::int as unique_outbound_clicks,
		
		sum(reach)::bigint as reach,
		sum(impressions)::bigint as impressions,
		
	  	round((sum(spend) / nullif(sum(unique_outbound_clicks), 0))::numeric, 2) as cpc,
	  	round((sum(unique_outbound_clicks) / nullif(sum(reach), 0)::float)::numeric, 4) as ctr
		
	from ads
		
	{% if daily or (agg_layers != '') -%}
        group by
    {%- endif %}

    {% if daily -%}
        date_start
        {% if (agg_layers != '') -%}
            ,
        {%- endif %}
    {%- endif %}

    {% if (agg_layers != '') -%}
        {{agg_layers[:-1]}}
    {%- endif %}
        
)

select * 
from final

{% if daily or (agg_layers != '') -%}
    order by
{%- endif %}

{% if daily -%}
    date_start desc
    {% if (agg_layers != '') -%}
        ,
    {%- endif %}
{%- endif %}

{% if (agg_layers != '') -%}
    {{agg_layers[:-1]}}
{%- endif %}
    
{% endmacro %}