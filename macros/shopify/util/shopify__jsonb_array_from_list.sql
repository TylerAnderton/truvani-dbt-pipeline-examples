{% macro shopify__jsonb_array_from_list(items_list, type) %}

{% set jsonb_array=[] %}

{% for item in items_list %}
    {% if type == 'sku' %}
        {% do jsonb_array.append('\'[{"sku": "' ~ item|string ~ '"}]\'::jsonb') %}    
    {% endif %}
    {% if type == 'code' %}
        {% do jsonb_array.append('\'[{"code": "' ~ item|string ~ '"}]\'::jsonb') %}
    {% endif %}
    
{% endfor %}

{% set jsonb_array_string='ARRAY[\n' ~ jsonb_array|join(',\n') ~ '\n]' %}

{{ return(jsonb_array_string) }}
    
{% endmacro %}