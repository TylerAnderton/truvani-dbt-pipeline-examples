{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync'
) }}

with

shopify_fulfillments as (

    select

        -- orders.id as order_id, -- taking from fulfillment instead
        orders.name as order_name,

        orders.created_at as order_created_at,
        orders.created_at_pt as order_created_at_pt,

        (fill ->> 'id')::int8 as id,
        (fill ->> 'name') as name,
        lower((fill ->> 'status')) as status,
        fill -> 'receipt' as receipt,
        lower((fill ->> 'service')) as service,
        (fill ->> 'order_id')::int8 as order_id,
        (fill ->> 'created_at')::timestamptz as created_at,
        fill -> 'line_items' as line_items,
        (fill ->> 'updated_at')::timestamptz as updated_at,
        (fill ->> 'location_id')::int8 as location_id,
        lower((fill ->> 'tracking_url')) as tracking_url,
        lower((fill ->> 'shipment_status')) as shipment_status,
        (fill ->> 'tracking_number') as tracking_number,
        lower((fill ->> 'tracking_company')) as tracking_company,

        (fill ->> 'created_at')::timestamptz at time zone 'america/los_angeles' as created_at_pt,
        (fill ->> 'updated_at')::timestamptz at time zone 'america/los_angeles' as updated_at_pt

    from 
        {{ ref('stg_shopify__orders') }} as orders,
        jsonb_array_elements(fulfillments) as fill

)

select * from shopify_fulfillments