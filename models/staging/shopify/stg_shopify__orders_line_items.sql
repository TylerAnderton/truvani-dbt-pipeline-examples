with

shopify_orders_line_items as (

    select

        -- _airbyte_shopify_orders_hashid,

        orders.id as order_id,
        orders.name as order_name,
        orders.created_at as order_created_at,
        orders.created_at_pt as order_created_at_pt,

        (line_item ->> 'id')::int8 as id,
        (line_item ->> 'sku') as sku,
        (line_item ->> 'name') as name,
        (line_item ->> 'grams')::int8 as grams,
        (line_item ->> 'price')::float8 as price,
        (line_item ->> 'title') as title,
        line_item -> 'duties' as duties,
        lower((line_item ->> 'vendor')) as vendor,
        (line_item ->> 'taxable')::bool as taxable,
        (line_item ->> 'quantity')::int8 as quantity,
        (line_item ->> 'gift_card')::bool as gift_card,
        line_item -> 'price_set' as price_set, -- should always equal `price` value
        line_item -> 'tax_lines' as tax_lines,
        (line_item ->> 'product_id')::int8 as product_id,
        line_item -> 'properties' as properties,
        (line_item ->> 'variant_id')::int8 as variant_id,
        (line_item ->> 'pre_tax_price')::float8 as pre_tax_price,
        (line_item ->> 'variant_title') as variant_title,
        (line_item ->> 'product_exists')::bool as product_exists,
        (line_item ->> 'total_discount')::float8 as total_discount,
        line_item -> 'origin_location' as origin_location,
        (line_item ->> 'requires_shipping')::bool as requires_shipping,
        lower((line_item ->> 'fulfillment_status')) as fulfillment_status,
        line_item -> 'total_discount_set' as total_discount_set,
        lower((line_item ->> 'fulfillment_service')) as fulfillment_service,
        (line_item ->> 'admin_graphql_api_id') as admin_graphql_api_id,
        line_item -> 'destination_location' as destination_location,
        line_item -> 'discount_allocations' as discount_allocations,
        (line_item ->> 'fulfillable_quantity')::int8 as fulfillable_quantity,
        lower((line_item ->> 'variant_inventory_management')) as variant_inventory_management

        -- _airbyte_line_items_hashid

    from 
        {{ ref('stg_shopify__orders') }} as orders,
        jsonb_array_elements(line_items) as line_item

)

select * from shopify_orders_line_items