{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync'
) }}

with

shopify_order_refunds as (

    select

        -- orders.id as order_id, -- taking from fulfillment instead
        orders.name as order_name,

        orders.created_at as order_created_at,
        orders.created_at_pt as order_created_at_pt,

        (refund ->> 'id')::int8 as id,
        (refund ->> 'note') as note,
        refund -> 'duties' as duties,
        (refund ->> 'restock')::bool as restock,
        (refund ->> 'user_id')::int8 as "user_id",
        (refund ->> 'order_id')::int8 as order_id,
        (refund ->> 'created_at')::timestamptz as created_at,
        (refund ->> 'processed_at')::timestamptz as processed_at,
        refund -> 'transactions' as transactions,
        refund -> 'total_duties_set' as total_duties_set,
        refund -> 'order_adjustments' as order_adjustments,
        refund -> 'refund_line_items' as line_items,
        (refund ->> 'admin_graphql_api_id') as admin_graphql_api_id,

        (refund ->> 'created_at')::timestamptz at time zone 'america/los_angeles' as created_at_pt,
        (refund ->> 'processed_at')::timestamptz at time zone 'america/los_angeles' as processed_at_pt

    from 
        {{ ref('stg_shopify__orders') }} as orders,
        jsonb_array_elements(refunds) as refund

)

select * from shopify_order_refunds