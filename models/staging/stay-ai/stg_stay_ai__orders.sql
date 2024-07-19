with

stay_ai_orders as (

    select distinct

        "orderName" as order_name,
        substring("orderId" from '[0-9]+$')::bigint as order_id,
        substring("subscriptionId" from '[0-9]+$')::bigint as subscription_id,
    
        "customerId" as customer_id,
        "address",

        date_trunc('second', to_timestamp("createdAt", 'yyyy-mm-dd"t"hh24:mi:ss.ms"z"') at time zone 'utc')::timestamptz as created_at, -- created_at::date differs from shopify on only 8 entries as of 1/4/2024
        date_trunc('second', to_timestamp("updatedAt", 'yyyy-mm-dd"t"hh24:mi:ss.ms"z"') at time zone 'utc')::timestamptz as updated_at,

        "totalPrice" as total_price,
        "cartDiscountAmount" as total_discount,
        "totalShippingPrice" as shipping_price,
        "currentTotalTax" as current_total_tax,

        "lineItems" as line_items,
        "tags",

        lower("fulfillmentStatus") as fulfillment_status,

        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta
        
    from {{ source('stay_ai', 'stayai_v2_Orders') }}

),

stay_ai_orders_trans as (

    select 

        *,

        created_at at time zone 'america/los_angeles' as created_at_pt,
        updated_at at time zone 'america/los_angeles' as updated_at_pt

    from stay_ai_orders

)

select * from stay_ai_orders_trans