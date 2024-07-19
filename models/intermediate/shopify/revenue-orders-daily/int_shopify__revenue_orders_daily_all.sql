with 

orders AS (

    select * from {{ ref('stg_shopify__orders') }}

),

final as (

    select
        created_at_pt::date as date_pst,
        round(sum(total_price)::numeric, 2) as total,
        count(*) as order_count
    from
        orders
    group by
        date_pst 

)

select * 
from final
order by date_pst desc