with 

tiktok_ads_spend as (

    select * from {{ ref('stg_tiktok__ads_report_daily') }}

),

final as (

    select 
        report_date,
        round(sum(spend)::numeric, 2) as spend

    from tiktok_ads_spend

    group by 
        report_date
    
)

select *
from final
order by report_date desc