{% macro clickfunnels__utm_contents_exact_filter(utm_contents_exact) %}
    {% set filter_contents = [] %}

    {% for content in utm_contents_exact %}
        {% do filter_contents.append("'" ~ content ~ "'") %}
    {% endfor %}

    {% set utm_content_filter = filter_contents|join(', ') %}
    
    {{ return("lower(utm_content) in (" ~ utm_content_filter ~ ")") }}
{% endmacro %}
