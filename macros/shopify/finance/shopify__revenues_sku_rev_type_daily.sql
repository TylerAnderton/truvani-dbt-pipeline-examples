{% macro shopify__revenues_sku_rev_type_daily(
    line_items_model,
    datetime_field,
    b2b=false
) %}

{{ config(
    materialized='incremental',
    unique_key='unique_id',
    on_schema_change='sync'
) }}

with 

line_items as (

    select *
    from {{ ref(line_items_model) }}
    where 
        {% if b2b -%} app_id = 1150484 {% else -%} app_id <> 1150484 {%- endif %}
        and line_item_price <> 0
        and line_item_sku !~* '-R$'

),

revenues as (

    select
        {{datetime_field}}::date as date,
        line_item_sku,
        line_item_revenue_type,

        round(sum(line_item_quantity)::numeric) as line_item_quantity,
        round(sum(line_item_price * line_item_quantity)::numeric, 2) as line_item_gross_revenue,
        round(sum(line_item_discount)::numeric, 2) as line_item_discount        

    from line_items
    group by
        date,
        line_item_sku,
        line_item_revenue_type

),

refunds as (

    select 
        refunded_at_pt::date as date,
        line_item_sku,
        line_item_revenue_type,

        round(sum(line_item_refund_quantity)::numeric) as line_item_refund_quantity,
        round(sum(line_item_refund_subtotal)::numeric, 2) as line_item_refund -- subtotal includes quantity * price
    from line_items
    group by
        date,
        line_item_sku,
        line_item_revenue_type
    
),

net_revenues as (

    select
        coalesce(revenues.date, refunds.date) as date,
        coalesce(revenues.line_item_sku, refunds.line_item_sku) as line_item_sku,
        coalesce(revenues.line_item_revenue_type, refunds.line_item_revenue_type) as line_item_revenue_type,

        coalesce(revenues.line_item_quantity, 0) as line_item_quantity,
        coalesce(refunds.line_item_refund_quantity, 0) as line_item_refund_quantity,
        coalesce(revenues.line_item_gross_revenue, 0) as line_item_gross_revenue,
        coalesce(revenues.line_item_discount, 0) as line_item_discount,
        coalesce(refunds.line_item_refund, 0) as line_item_refund,
        coalesce(revenues.line_item_gross_revenue, 0) - coalesce(revenues.line_item_discount, 0) - coalesce(refunds.line_item_refund, 0) as line_item_net_revenue

    from revenues
    full join refunds 
        using (
            date,
            line_item_sku,
            line_item_revenue_type
        )
    where date is not null

)

select 
    date::text || coalesce(line_item_sku, 'OTHER') || line_item_revenue_type as unique_id, -- unique_id for incremental updates
    *
from net_revenues
order by 
    date desc,
    line_item_sku asc,
    line_item_revenue_type asc
    
{% endmacro %}