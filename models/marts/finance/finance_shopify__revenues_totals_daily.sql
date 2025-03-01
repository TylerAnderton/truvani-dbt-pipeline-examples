{{ config(
    materialized='incremental',
    unique_key='date',
    on_schema_change='sync'
) }}

with 

line_items as (

    select *
    from {{ ref('int_shopify__orders_line_items_refunds') }}
    where app_id <> 1150484

),

orders as (

    select distinct
        order_name,
        ordered_at_pt,
        first_fulfillment_date,
        app_id,
        app_name,

        total_line_items_price as gross_revenue,
        total_discounts, -- includes item discounts, shipping discounts, and order discounts
        order_discounts, -- some order discounts are actually shipping discounts
        total_discounts - order_discounts - shipping_discounts as item_discounts,
        subtotal_price as subtotal_revenue, -- gross revenue - (item discounts + order discounts) (not including shipping discounts)

        shipping_price as shipping_revenue,
        shipping_discounts,
        discounted_shipping_price as discounted_shipping_revenue,

        total_tax,
        total_price as total_revenue -- subtotal + discounted_shipping + total_tax = gross_revenue - total_discounts + shipping + total_tax

    from line_items
    where line_item_sku !~* '-R'

),

revenues as (

    select
        first_fulfillment_date::date as date,

        round(sum(gross_revenue)::numeric, 2) as gross_revenue,
        
        round(sum(total_discounts)::numeric, 2) as total_discounts,
        round(sum(total_discounts)::numeric, 2) - round(sum(shipping_discounts)::numeric, 2) as total_minus_ship_discounts, -- matches discounts in Shopify report
        round(sum(order_discounts)::numeric, 2) as order_discounts,
        round(sum(item_discounts)::numeric, 2) as item_discounts,

        round(sum(subtotal_revenue)::numeric, 2) as subtotal_revenue,

        round(sum(shipping_revenue)::numeric, 2) as shipping_revenue,
        round(sum(shipping_discounts)::numeric, 2) as shipping_discounts,
        round(sum(discounted_shipping_revenue)::numeric, 2) as discounted_shipping_revenue, -- matches shipping in Shopify report

        round(sum(total_tax)::numeric, 2) as total_tax,
        round(sum(total_revenue)::numeric, 2) as total_revenue

    from orders
    group by
        date

),

refunds as (

    select 
        first_fulfillment_date::date as date,

        round(sum(line_item_refund_subtotal)::numeric, 2) as refund_subtotal,
        round(sum(line_item_refund_tax)::numeric, 2) as refund_tax,
        round(sum(line_item_refund_total)::numeric, 2) as refund_total

    from line_items
    group by
        date

),

joined as (

    select
        revenues.date,

        revenues.gross_revenue,

        revenues.total_discounts,
        revenues.total_minus_ship_discounts,

        refunds.refund_subtotal,
        
        revenues.subtotal_revenue,

        revenues.shipping_revenue,
        revenues.shipping_discounts,
        revenues.discounted_shipping_revenue,

        revenues.total_tax,
        refunds.refund_tax,
        revenues.total_tax - refunds.refund_tax as net_tax,

        revenues.total_revenue,
        refunds.refund_total,
        revenues.total_revenue - refunds.refund_total as net_total_revenue

    from revenues
    left join refunds using (date)

)

select *
from joined
where date is not null
order by 
    date desc