with 

facebook as (

    select * from {{ ref('int_facebook__spend_daily_channel') }}

),

google_ads as (

    select * from {{ ref('int_google_ads__spend_daily_channel') }}

),

tiktok as (

    select * from {{ ref('int_tiktok__spend_daily_channel') }}

),

microsoft as (

    select * from {{ ref('int_microsoft__spend_daily_channel') }}

),

influencers as (

    select * from {{ ref('int_influencers__spend_daily_channel') }}

),

post_pilot as (

    select * from {{ ref('int_post_pilot__spend_daily_channel') }}

),

final as (

    select 
        coalesce(
            facebook.report_date,
            google_ads.report_date,
            tiktok.report_date,
            microsoft.report_date,
            influencers.report_date,
            post_pilot.report_date
        ) as report_date,
        
        round(
            (
                coalesce(facebook.spend, 0) 
                + coalesce(google_ads.spend, 0) 
                + coalesce(tiktok.spend, 0) 
                + coalesce(microsoft.spend, 0) 
                + coalesce(influencers.spend, 0) 
                + coalesce(post_pilot.spend, 0)
            )::numeric,
            2
        ) as spend
            
    from facebook

    full join google_ads 
        using (report_date)

    full join tiktok
        using (report_date)

    full join microsoft
        using (report_date)

    full join influencers
        using (report_date)

    full join post_pilot
        using (report_date)

)

select *
from final
order by report_date desc