{% macro facebook__spend(
    attn_truvani=None,
    campaign_name_includes=[],
    campaign_name_excludes=[],
    daily=False,
    agg_level=None,
    start_date=None,
    end_date=None,
    account='main'
) %}


{% set regex_include=misc__join_regex_terms(campaign_name_includes) %}
{% set regex_exclude=misc__join_regex_terms(campaign_name_excludes) %}


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


{% set source_model='stg_facebook_main__ads_insights' %}

{% if account == 'he1' or account == 'health' %}
    {% set source_model='stg_facebook_he1__ads_insights' %}
{% endif %}


with facebook_ads_insights as (

    select * from {{ ref(source_model) }}

),

final as (

    select 
        {% if daily -%}
            date_start as report_date,
        {% endif -%}

        {{agg_layers}}

        round(sum(spend)::numeric, 2) as spend

    from facebook_ads_insights

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
                
    {% if daily or (agg_layers != '') -%}
        group by
    {%- endif %}

    {% if daily -%}
        report_date
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
    report_date desc
    {% if (agg_layers != '') -%}
        ,
    {%- endif %}
{%- endif %}

{% if (agg_layers != '') -%}
    {{agg_layers[:-1]}}
{%- endif %}
    
{% endmacro %}