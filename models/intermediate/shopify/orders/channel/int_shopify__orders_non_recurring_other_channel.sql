with 

shopify_orders as (

    select * from {{ ref('stg_shopify__orders') }}

),

attributed_orders as (

    {{ dbt_utils.union_relations(
        relations=[
            ref('int_shopify__orders_non_recurring_facebook_channel'),
            ref('int_shopify__orders_non_recurring_tiktok_channel'),
            ref('int_shopify__orders_non_recurring_google_channel'),
            ref('int_shopify__orders_non_recurring_microsoft_channel')
        ],
        include=[
            "name",
            "created_at"
        ],
        source_column_name=None
    ) }}

),

final as (

    select 
        shopify_orders.*

    from shopify_orders

    left join attributed_orders 
    on
        shopify_orders.name = attributed_orders.name
        and shopify_orders.created_at = attributed_orders.created_at
        
    where 
        attributed_orders.name is null
        and shopify_orders.app_id not in (2820951,11924930561)

)

select *
from final
order by created_at desc