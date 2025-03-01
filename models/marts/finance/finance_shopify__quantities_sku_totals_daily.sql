{{ config(
    materialized='incremental',
    unique_key='unique_id',
    on_schema_change='sync'
) }}

with 

total_quantities as (

    select
        date,
        line_item_sku,
        sum(total_quantity) as total_quantity,
        sum(expensed_quantity) as expensed_quantity
    from {{ ref('finance_shopify__quantities_sku_rev_type_daily') }}
    group by
        date,
        line_item_sku

)

select 
    date::text || line_item_sku as unique_id, -- unique_id for incremental updates
    *
from total_quantities
order by 
    date desc,
    line_item_sku asc