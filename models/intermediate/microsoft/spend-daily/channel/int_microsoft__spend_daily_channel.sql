with 

microsoft_campaigns_spend as (

    select * from {{ ref('stg_microsoft__campaigns_report_daily') }}

),

final as (

    select 
        report_date,
        round(sum(spend)::numeric, 2) as spend

    from microsoft_campaigns_spend

    group by 
        report_date
    
)

select *
from final
order by report_date desc