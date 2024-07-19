{% macro clickfunnels__unique_order_amounts_filter(unique_order_amounts) %}
    {% set filter_conditions = [] %}

    {% for amount in unique_order_amounts %}
        {% do filter_conditions.append('subtotal = ' ~ amount|string) %}
    {% endfor %}

    {% set unique_order_amounts_filter = filter_conditions|join(' or ') %}
    
    {{ return(unique_order_amounts_filter) }}
{% endmacro %}
