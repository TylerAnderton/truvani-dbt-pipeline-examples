{% macro shopify__orders_non_recurring_discount_code_regex(
    regex_include_list=[],
    regex_exclude_list=[]
) -%}

with

final as (

    select
        *
    from 
        {{ ref('stg_shopify__orders') }}
    where
        -- standard conditions
        tags !~* 'subscription recurring order|active subscription'
        and app_id <> 5859381 -- not Stay AI recurring orders
        and cancelled_at is null
        -- check for inclusion regex matches
        {% if regex_include_list|length > 0 %}
            and exists (
                select 1
                from jsonb_array_elements(discount_codes) as discount_code
                where (
                    {% for regex in regex_include_list %}
                        discount_code->>'code' ~* '{{ regex }}'
                        {% if not loop.last %} or {% endif %}
                    {% endfor %}
                )
            )
        {% endif %}
        -- check for exclusion regex matches
        {% if regex_exclude_list|length > 0 %}
            and not exists (
                select 1
                from jsonb_array_elements(discount_codes) as discount_code
                where (
                    {% for regex in regex_exclude_list %}
                        discount_code->>'code' ~* '{{ regex }}'
                        {% if not loop.last %} or {% endif %}
                    {% endfor %}
                )
            )
        {% endif %}
        
)

select * 
from final
order by created_at_pt desc

{%- endmacro %}