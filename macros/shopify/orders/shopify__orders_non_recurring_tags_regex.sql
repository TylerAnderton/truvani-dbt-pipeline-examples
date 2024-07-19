{% macro shopify__orders_non_recurring_tags_regex(
    regex_include_list=[],
    regex_exclude_list=[]
) -%}

with

shopify_orders as (

    select * from {{ ref('stg_shopify__orders') }}

),

final as (

    select
        *
    from 
        shopify_orders 
    where
        {% if regex_include_list|length > 0 -%}
            {% for regex in regex_include_list -%}
                tags ~* {{regex}}
                and 
            {% endfor %}
        {%- endif %}

        {% if regex_exclude_list|length > 0 -%}
            {% for regex in regex_exclude_list -%}
                tags !~* {{regex}}
                and 
            {% endfor %}
        {%- endif %}

        tags !~* 'subscription recurring order'
        and tags !~* 'active subscription'
        and cancelled_at is null
)

select * 
from final
order by created_at_pt desc

{%- endmacro %}