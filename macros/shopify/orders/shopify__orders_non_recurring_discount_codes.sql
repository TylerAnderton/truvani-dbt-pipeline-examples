{% macro shopify__orders_non_recurring_discount_codes(discount_codes) -%}

{% set code_jsonb_array=shopify__jsonb_array_from_list(items_list=discount_codes, type='code') -%}

with

final as (

    select
        *
    from 
        {{ ref('stg_shopify__orders') }} 
    where
        discount_codes @> ANY({{ code_jsonb_array }})
        and tags !~* 'subscription recurring order|active subscription'
        and app_id <> 5859381 -- not Stay AI recurring orders
        and cancelled_at is null
)

select * 
from final
order by created_at_pt desc

{%- endmacro %}