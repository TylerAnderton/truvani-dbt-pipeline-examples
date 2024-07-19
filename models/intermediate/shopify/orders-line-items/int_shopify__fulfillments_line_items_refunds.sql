with

oli_labeled as (

    select *
    from {{ ref('int_shopify__fulfillments_line_items_labeled') }}

),

-- refunds as (


--     select *
--     from {# ref('stg_shopify__orders_refunds') #}

-- ),

-- refund_line_items as (

--     select *
--     from {# ref('stg_shopify__orders_refunds_line_items') #}

-- ),

rli_labeled as (

    select 
        -- refunds._airbyte_shopify_orders_hashid,
        -- refunds.created_at_pt as refunded_at_pt,

        order_id,

        refund_id,
        refund_created_at_pt as refunded_at_pt,

        sku as line_item_sku,
        quantity as line_item_quantity,
        subtotal as line_item_refund_subtotal,
        total_tax as line_item_refund_tax,
        subtotal + total_tax as line_item_refund_total

    from {{ ref('stg_shopify__orders_refunds_line_items') }}

),

joined_oli as (

    select 
        oli_labeled.*,
        rli_labeled.refunded_at_pt,
        rli_labeled.line_item_quantity as line_item_refund_quantity,
        coalesce(rli_labeled.line_item_refund_subtotal, 0) as line_item_refund_subtotal,
        coalesce(rli_labeled.line_item_refund_tax, 0) as line_item_refund_tax,
        coalesce(rli_labeled.line_item_refund_total, 0) as line_item_refund_total
    from oli_labeled
    left join rli_labeled
        using (order_id, line_item_sku)

)

select * 
from joined_oli
order by 
    fulfilled_at_pt desc,
    order_name desc,
    line_item_price desc,
    line_item_name asc