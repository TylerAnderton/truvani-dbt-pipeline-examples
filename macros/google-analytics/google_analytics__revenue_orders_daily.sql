{% macro google_analytics__revenue_orders_daily(
    campaign_name_includes=[],
    campaign_name_excludes=[],
    source_regex=None
) %}

{% set campaign_regex_include=misc__join_regex_terms(campaign_name_includes) %}
{% set campaign_regex_exclude=misc__join_regex_terms(campaign_name_excludes) %}

with metrics as (

    select * 
    from {{ ref('stg_google_analytics__campaign_metrics') }}
    where source ~* 'google'

),

final as (

    select
        report_date as date_pst,
        round(sum(revenue)::numeric, 2) as total,
        round(sum(conversions)::numeric, 0) as order_count

    from metrics

    {% if (campaign_name_includes|length > 0) or (campaign_name_excludes|length > 0) or source_regex -%}
       where 
    {%- endif %}

    {% if (campaign_name_includes|length > 0) -%}

        {% for regex in campaign_name_includes -%}
            campaign_name ~* '{{regex}}'
            {% if not loop.last -%}
                and
            {% endif %}
        {%- endfor %}

        {%- if (campaign_name_excludes|length > 0) or source_regex %}
            and
        {% endif %}
    {%- endif %}
        
    {% if (campaign_name_excludes|length > 0) -%}

        {% for regex in campaign_name_excludes -%}
            campaign_name !~* '{{regex}}'
            {% if not loop.last -%}
                and
            {% endif %}
        {%- endfor %}

        {%- if source_regex %}
            and
        {% endif %}
    {%- endif %}

    {% if source_regex %}
        source ~* {{source_regex}}
    {% endif %}

    group by date_pst 

)

select *
from final
where order_count > 0
order by date_pst desc
    
{% endmacro %}