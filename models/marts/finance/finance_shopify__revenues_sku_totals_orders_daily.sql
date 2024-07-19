with 

rev_type_revenues as (

    select *
    from {{ ref('finance_shopify__revenues_sku_rev_type_orders_daily') }}

),

total_revenues as (

    select
        date,
        line_item_sku,
        coalesce(sum(line_item_quantity), 0) as line_item_quantity,
        coalesce(sum(line_item_gross_revenue), 0) as line_item_gross_revenue,
        coalesce(sum(line_item_discount), 0) as line_item_discount,
        coalesce(sum(line_item_refund), 0) as line_item_refund,
        coalesce(sum(line_item_gross_revenue), 0) - coalesce(sum(line_item_discount), 0) - coalesce(sum(line_item_refund), 0) as line_item_net_revenue
    from rev_type_revenues
    group by
        date,
        line_item_sku

)

select *
from total_revenues
order by 
    date desc,
    line_item_sku asc