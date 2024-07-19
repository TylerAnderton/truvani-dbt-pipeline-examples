{% macro clickfunnels__utm_terms_exact_filter(utm_terms_exact) %}
    {% set filter_terms = [] %}

    {% for term in utm_terms_exact %}
        {% do filter_terms.append("'" ~ term ~ "'") %}
    {% endfor %}

    {% set utm_term_filter = filter_terms|join(', ') %}
    
    {{ return("lower(utm_term) in (" ~ utm_term_filter ~ ")") }}
{% endmacro %}
