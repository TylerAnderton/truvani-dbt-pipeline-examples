{{ config(
    materialized='incremental',
    unique_key='unique_id',
    on_schema_change='sync'
) }}

with 

total_revenues as (

    select
        date,
        line_item_sku,
        coalesce(sum(line_item_quantity), 0) as line_item_quantity,
        coalesce(sum(line_item_gross_revenue), 0) as line_item_gross_revenue,
        coalesce(sum(line_item_discount), 0) as line_item_discount,
        coalesce(sum(line_item_refund), 0) as line_item_refund,
        coalesce(sum(line_item_gross_revenue), 0) - coalesce(sum(line_item_discount), 0) - coalesce(sum(line_item_refund), 0) as line_item_net_revenue
    from {{ ref('finance_shopify__revenues_sku_rev_type_daily') }}
    group by
        date,
        line_item_sku

)

select 
    date::text || line_item_sku as unique_id, -- unique_id for incremental updates
    *
from total_revenues
order by 
    date desc,
    line_item_sku asc