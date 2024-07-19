{% macro shopify__orders_non_recurring_skus(sku_list) -%}

{% set sku_jsonb_array=shopify__jsonb_array_from_list(items_list=sku_list, type='sku') -%}

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
        line_items @> ANY({{ sku_jsonb_array }})
        and tags !~* 'subscription recurring order'
        and tags !~* 'active subscription'
        and cancelled_at is null
)

select * 
from final
order by created_at_pt desc

{%- endmacro %}