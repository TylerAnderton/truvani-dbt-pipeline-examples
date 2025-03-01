{{ config(
    materialized='incremental',
    unique_key='oli_unique_id',
    on_schema_change='sync'
) }}

{% set apps={
    "3613079":"App 1",
    "88312":"App 2",
    "1356615":"App 3",
    "188777":"App 4",
    "1558585":"App 5",
    "1354745":"App 6"
} %}

with 

orders as (

    select 
        *,
        id as order_id
    from {{ ref('stg_shopify__orders') }}
    where 
        cancelled_at is null
        and name is not null

),

-- line_items as (

--     select *
--     from {{ ref('stg_shopify__orders_line_items') }}

-- ),

fulfillments as (

    select *
    from {{ ref('stg_shopify__orders_fulfillments') }}

),

fulfillments_line_items as (

    select *
    from {{ ref('stg_shopify__orders_fulfillments_line_items') }}

),

shipping_lines as (

    select *
    from {{ ref('stg_shopify__orders_shipping_lines') }}

),

shipping_totals as (

    select
        order_id,
        sum(discounted_price) as discounted_shipping_price,
        sum(price) as shipping_price
    from shipping_lines
    group by order_id

),

fulfillments_line_items_join as (

    select

        (fulfillments.id || line_items.unique_id) as fulfillment_item_id, -- unique_id for incremental updates

        fulfillments.id as fulfillments_id,
        fulfillments.name as fulfillment_name,
        fulfillments.status as fulfillment_status,
        fulfillments.created_at_pt as fulfilled_at_pt,

        case
            when line_items.sku = '' and line_items.name ~* 'The Only Bar \(Chocolate Peanut Butter\) - 1 Bar - Replacement' then 'OB-CPB-1-R'
            when line_items.sku = '' and line_items.name ~* 'The Only Bar \(Chocolate Peanut Butter\) - 1 Bar' then 'OB-CP-1'
            when line_items.sku = '' and line_items.name ~* 'The Only Bar \(Mint Chocolate\) - 1 Bar' then 'OB-MC-1'
            else line_items.sku
        end as line_item_sku,

        line_items.name as line_item_name,
        line_items.price as line_item_price,
        line_items.quantity as line_item_quantity,

        coalesce((
            select sum((discount_allocation->>'amount')::numeric)
            from jsonb_array_elements(line_items.discount_allocations) as discount_allocation
        ), 0) as line_item_discount, -- total order discounts allocated to individual line_items -- does not include shipping discounts


        order_id

    from fulfillments
    left join fulfillments_line_items as line_items using (order_id)
    where fulfillments.status <> 'cancelled'

),

orders_line_items as (

    select 

        (orders.order_id::text || coalesce(line_items.fulfillment_item_id, 'NULL')) as oli_unique_id, -- unique_id for incremental updates

        orders.order_id,
        orders.name as order_name,
        orders.created_at_pt as ordered_at_pt,
        line_items.fulfilled_at_pt,
        orders.app_id,

        case
			{% for id, name in apps.items() -%}
                when orders.app_id = {{id}} then '{{name}}'
            {% endfor -%}
			else 'Other'
		end as app_name,
        orders.tags,
        -- orders.refunds,

        orders.total_line_items_price, -- sum of line_items prices (gross revenue)
        orders.total_discounts, -- includes item discounts, shipping discounts, and order discounts

        round(
            (
                orders.total_discounts 
                - (orders.total_line_items_price - orders.subtotal_price) 
                - (coalesce(shipping.shipping_price, 0) - coalesce(shipping.discounted_shipping_price, 0))
            )::numeric
            , 2
        ) as order_discounts, -- total_discounts - item_discounts - shipping_discounts -- order_discounts can be allocated to each SKU, or just apply to shipping

        orders.subtotal_price, -- gross revenue - (item discounts + order discounts) (not including shipping discounts)

        coalesce(shipping.shipping_price, 0) as shipping_price,
        coalesce(shipping.shipping_price, 0) - coalesce(shipping.discounted_shipping_price, 0) as shipping_discounts,
        coalesce(shipping.discounted_shipping_price, 0) as discounted_shipping_price,

        orders.total_tax,
        orders.total_price, -- subtotal + discounted_shipping + total_tax
       
        line_items.fulfillment_name,
        line_items.fulfillment_status,
        line_items.line_item_sku,
        line_items.line_item_name,
        line_items.line_item_quantity,
        line_items.line_item_price,  -- sku gross_revenue
        line_items.line_item_discount

    from orders
    left join fulfillments_line_items_join as line_items using (order_id) 
    left join shipping_totals as shipping using (order_id)
    -- where line_items.fulfillment_name is not null -- CONSIDER REMOVING --

),

oli_labeled as (

    select 

        *,

        case
            when app_id = 1150484 then 'wholesale'
            when (line_item_name !~* 'monthly subscription' and app_id not in (2820951, 11924930561) and app_id <> 1150484) then 'otp'
            when (line_item_name !~* 'monthly subscription' and app_id in (2820951, 11924930561) and app_id <> 1150484) then 'funnelish'
            when (line_item_name ~* 'monthly subscription' and app_id not in (2820951, 11924930561) and app_id <> 1150484 and tags ~* 'first subscription|subscription first order') then 'subscription_first'
            when (line_item_name ~* 'monthly subscription' and app_id not in (2820951, 11924930561) and app_id <> 1150484 and tags ~* 'active subscription|subscription recurring order') then 'subscription_recurring'
            else 'other'
        end as line_item_revenue_type

    from orders_line_items

)

select * 
from oli_labeled
order by 
    fulfilled_at_pt desc,
    ordered_at_pt desc,
    order_name desc,
    line_item_price desc,
    line_item_name asc