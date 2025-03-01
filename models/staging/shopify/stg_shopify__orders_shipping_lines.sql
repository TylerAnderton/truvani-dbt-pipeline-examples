{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync'
) }}

with

shopify_orders_shipping_lines as (

    select

        orders.id as order_id,
        orders.name as order_name,
        orders.created_at as order_created_at,
        orders.created_at_pt as order_created_at_pt,
        
        (shipping_line ->> 'id')::int8 as id,
        lower(shipping_line ->> 'code') as code,
        (shipping_line ->> 'price')::float8 as price,
        lower(shipping_line ->> 'title') as title,
        lower(shipping_line ->> 'source') as source,
        shipping_line -> 'price_set' as price_set,
        shipping_line -> 'tax_lines' as tax_lines,
        (shipping_line ->> 'discounted_price')::float8 as discounted_price,
        lower(shipping_line ->> 'carrier_identifier') as carrier_identifier,
        shipping_line -> 'discount_allocations' as discount_allocations,
        shipping_line -> 'discounted_price_set' as discounted_price_set,
        lower(shipping_line ->> 'requested_fulfillment_service_id') as requested_fulfillment_service_id

    from 
        {{ ref('stg_shopify__orders') }} as orders,
        jsonb_array_elements(shipping_lines) as shipping_line

)

select * from shopify_orders_shipping_lines