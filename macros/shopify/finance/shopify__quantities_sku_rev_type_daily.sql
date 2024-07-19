{% macro shopify__quantities_sku_rev_type_daily(
    line_items_model,
    datetime_field,
    b2b=false
) %}

with 

line_items as (

    select *
    from {{ ref(line_items_model) }}
    where {% if b2b -%} app_id = 1150484 {% else -%} app_id <> 1150484 {%- endif %}

),

total_quantities as (

    select
        {{datetime_field}}::date as date,
        line_item_sku,
        line_item_revenue_type,
        coalesce(sum(line_item_quantity), 0) as total_quantity
    from line_items
    group by
        date,
        line_item_sku,
        line_item_revenue_type

),

expensed_quantities as (

    select
        {{datetime_field}}::date as date,
        line_item_sku,
        line_item_revenue_type,
        coalesce(sum(line_item_quantity), 0) as expensed_quantity
    from line_items
    where 
        line_item_price = 0
        or line_item_sku ~* '-R$'
    group by
        date,
        line_item_sku,
        line_item_revenue_type

),

joined_quantities as (

    select
        totals.*,
        coalesce(expensed.expensed_quantity, 0) as expensed_quantity
    from total_quantities as totals
    left join expensed_quantities as expensed
        on totals.date = expensed.date
        and totals.line_item_sku = expensed.line_item_sku
        and totals.line_item_revenue_type = expensed.line_item_revenue_type
    where totals.date is not null

)

select *
from joined_quantities
order by 
    date desc,
    line_item_sku asc
    
{% endmacro %}