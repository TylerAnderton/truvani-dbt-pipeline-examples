with 

rev_type_quantities as (

    select *
    from {{ ref('finance_shopify__quantities_sku_rev_type_orders_daily') }}

),

total_quantities as (

    select
        date,
        line_item_sku,
        sum(total_quantity) as total_quantity,
        sum(expensed_quantity) as expensed_quantity
    from rev_type_quantities
    group by
        date,
        line_item_sku

)

select *
from total_quantities
order by 
    date desc,
    line_item_sku asc