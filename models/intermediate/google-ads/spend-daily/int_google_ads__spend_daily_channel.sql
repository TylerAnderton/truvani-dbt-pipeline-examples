with 

google_ads_spend as (

    select * from {{ ref('stg_google_ads__campaign_metrics') }}

),

final as (

    select 
        report_date,
        round(sum(spend)::numeric, 2) as spend

    from google_ads_spend

    group by 
        report_date
    
)

select *
from final
order by report_date desc