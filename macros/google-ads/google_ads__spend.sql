{% macro google_ads__spend(
    campaign_name_includes=[],
    campaign_name_excludes=[],
    campaign_types=[],
    landing_page_selector=False,
    landing_page_includes=[],
    landing_page_excludes=[],
    daily=False,
    campaigns=False,
    start_date=None,
    end_date=None
) %}


{% set campaign_regex_include=misc__join_regex_terms(campaign_name_includes) %}
{% set campaign_regex_exclude=misc__join_regex_terms(campaign_name_excludes) %}

{% set landing_page_regex_include=misc__join_regex_terms(landing_page_includes) %}
{% set landing_page_regex_exclude=misc__join_regex_terms(landing_page_excludes) %}

{% set quoted_types = [] %}

{% for type in campaign_types %}
    {% do quoted_types.append('\'' ~ type ~ '\'') %}
{% endfor %}

{% set types_filter = '(' ~ quoted_types|join(',') ~ ')' %}

with source as (

    select * 
    from 
        {% if landing_page_selector -%}
        {{ ref('stg_google_ads__campaigns_landing_pages') }}
        {%- else %}
        {{ ref('stg_google_ads__campaign_metrics') }}
        {%- endif %}
        
),

final as (

    select 
        {% if daily -%}
            report_date,
        {% endif -%}

        {% if campaigns -%}
            campaign_name,
        {% endif -%}

        round(sum(spend)::numeric, 2) as spend

    from source

    where
        spend <> 0


        {% if campaign_regex_include|length > 0 -%}

        and campaign_name ~* '{{campaign_regex_include}}'

        {%- endif %}

        {%- if campaign_regex_exclude|length > 0 -%}

        and campaign_name !~* '{{campaign_regex_exclude}}'

        {%- endif %}


        {% if campaign_types|length > 0 -%}

        and lower(campaign_type) in {{types_filter|lower}}

        {%- endif %}


        {% if landing_page_regex_include|length > 0 -%}

        and landing_page ~* '{{landing_page_regex_include}}'

        {%- endif %}

        {%- if landing_page_regex_exclude|length > 0 -%}

        and landing_page !~* '{{landing_page_regex_exclude}}'

        {%- endif %}


        {% if start_date and end_date -%}
            and report_date between make_date({{start_date}}) and make_date({{end_date}}) 
        {%- endif %}
                
    {% if daily or campaigns -%}
        group by
    {%- endif %}

    {% if daily -%}
        report_date
        {% if campaigns -%}
            ,
        {%- endif %}
    {%- endif %}

    {% if campaigns -%}
        campaign_name
    {%- endif %}
        
)

select * 
from final

{% if daily or campaigns -%}
    order by
{%- endif %}

{% if daily -%}
    report_date desc
    {% if campaigns -%}
        ,
    {%- endif %}
{%- endif %}

{% if campaigns -%}
    campaign_name
{%- endif %}
    
{% endmacro %}