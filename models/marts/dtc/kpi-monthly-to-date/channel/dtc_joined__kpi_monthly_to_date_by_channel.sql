-- {{ config(materialized='table') }}

with 

microsoft as (

    select * from {{ ref('dtc_joined__kpi_monthly_to_date_microsoft_channel') }}

),

facebook as (

    select * from {{ ref('dtc_joined__kpi_monthly_to_date_facebook_channel') }}

),

google as (

    select * from {{ ref('dtc_joined__kpi_monthly_to_date_google_channel') }}

),

tiktok as (

    select * from {{ ref('dtc_joined__kpi_monthly_to_date_tiktok_channel') }}

),

other as (

    select * from {{ ref('dtc_joined__kpi_monthly_to_date_other_channel') }}

),

final as (

    select 
        'Microsoft' as channel,
        *
    from microsoft
        
    union all

    select 
        'Facebook' as channel,
        *
    from facebook
        
    union all

    select 
        'Google' as channel,
        *
    from google
        
    union all
    
    select 
        'Other' as channel,
        *
    from other
        
    union all

    select 
        'TikTok' as channel,
        *
    from tiktok

)

select * 
from final
order by
	date_month desc,
	channel asc