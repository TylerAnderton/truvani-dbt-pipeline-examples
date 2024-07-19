{% macro clickfunnels__utm_campaigns_exact_filter(utm_campaigns_exact) %}
    {% set filter_campaigns = [] %}

    {% for campaign in utm_campaigns_exact %}
        {% do filter_campaigns.append("'" ~ campaign ~ "'") %}
    {% endfor %}

    {% set utm_campaign_filter = filter_campaigns|join(', ') %}
    
    {{ return("lower(utm_campaign) in (" ~ utm_campaign_filter ~ ")") }}
{% endmacro %}
