with

shopify_order_refunds_line_items as (

    select
        
        refunds.order_id,
        refunds.order_name,
        refunds.order_created_at,
        refunds.order_created_at_pt,

        refunds.id as refund_id,
        refunds.created_at as refund_created_at,
        refunds.created_at_pt as refund_created_at_pt,
        refunds.processed_at as refund_processed_at,
        refunds.processed_at_pt as refund_processed_at_pt,

        (line_item ->> 'subtotal')::float8 as subtotal, -- should = price * quantity
        (line_item ->> 'total_tax')::float8 as total_tax,
        round(
            (
                (line_item ->> 'subtotal')::float8 
                + (line_item ->> 'total_tax')::float8 
                - ((line_item -> 'line_item') ->> 'total_discount')::float8
            )::numeric,
            2
        ) as total, -- sometimes total < 0, idk why

        ((line_item -> 'line_item') ->> 'id')::int8 as id,
        ((line_item -> 'line_item') ->> 'sku') as sku,
        ((line_item -> 'line_item') ->> 'name') as name,
        ((line_item -> 'line_item') ->> 'grams')::int8 as grams,
        ((line_item -> 'line_item') ->> 'price')::float8 as price,
        ((line_item -> 'line_item') ->> 'title') as title,
        (line_item -> 'line_item') -> 'duties' as duties,
        lower(((line_item -> 'line_item') ->> 'vendor')) as vendor,
        ((line_item -> 'line_item') ->> 'taxable')::bool as taxable,
        ((line_item -> 'line_item') ->> 'quantity')::int8 as quantity,
        ((line_item -> 'line_item') ->> 'gift_card')::bool as gift_card,
        (line_item -> 'line_item') -> 'price_set' as price_set, -- should always equal `price` value
        (line_item -> 'line_item') -> 'tax_lines' as tax_lines,
        ((line_item -> 'line_item') ->> 'product_id')::int8 as product_id,
        (line_item -> 'line_item') -> 'properties' as properties,
        ((line_item -> 'line_item') ->> 'variant_id')::int8 as variant_id,
        ((line_item -> 'line_item') ->> 'pre_tax_price')::float8 as pre_tax_price,
        ((line_item -> 'line_item') ->> 'variant_title') as variant_title,
        ((line_item -> 'line_item') ->> 'product_exists')::bool as product_exists,
        ((line_item -> 'line_item') ->> 'total_discount')::float8 as total_discount,
        (line_item -> 'line_item') -> 'origin_location' as origin_location,
        ((line_item -> 'line_item') ->> 'requires_shipping')::bool as requires_shipping,
        lower(((line_item -> 'line_item') ->> 'fulfillment_status')) as fulfillment_status,
        (line_item -> 'line_item') -> 'total_discount_set' as total_discount_set,
        lower(((line_item -> 'line_item') ->> 'fulfillment_service')) as fulfillment_service,
        ((line_item -> 'line_item') ->> 'admin_graphql_api_id') as admin_graphql_api_id,
        (line_item -> 'line_item') -> 'destination_location' as destination_location,
        (line_item -> 'line_item') -> 'discount_allocations' as discount_allocations,
        ((line_item -> 'line_item') ->> 'fulfillable_quantity')::int8 as fulfillable_quantity,
        lower(((line_item -> 'line_item') ->> 'variant_inventory_management')) as variant_inventory_management

    from 
        {{ ref('stg_shopify__orders_refunds') }} as refunds,
        jsonb_array_elements(line_items) as line_item

)

select * from shopify_order_refunds_line_items