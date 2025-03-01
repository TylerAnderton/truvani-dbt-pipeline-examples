{{ config(
    materialized='incremental',
    unique_key='unique_id',
    on_schema_change='sync'
) }}

with 

facebook as (

    select * from {{ ref('dtc_joined__kpi_monthly_to_date_facebook_channel') }}

),

google as (

    select * from {{ ref('dtc_joined__kpi_monthly_to_date_google_channel') }}

),

tiktok as (

    select * from {{ ref('dtc_joined__kpi_monthly_to_date_tiktok_channel') }}

),

influencers as (

    select * from {{ ref('dtc_joined__kpi_monthly_to_date_influencers_channel') }}

),

post_pilot as (

    select * from {{ ref('dtc_joined__kpi_monthly_to_date_post_pilot_channel') }}

),

other as (

    select * from {{ ref('dtc_joined__kpi_monthly_to_date_other_channel') }}

),

agg as (

    -- select 
    --     'Bing' as channel,
    --     *
    -- from bing
        
    -- union all

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
        'Influencers' as channel,
        *
    from influencers
        
    union all

    select 
        'Other' as channel,
        *
    from other
        
    union all

    select 
        'Post Pilot' as channel,
        *
    from post_pilot
        
    union all

    select 
        'TikTok' as channel,
        *
    from tiktok

),

final as (

    select
        date_month::text || channel as unique_id, -- unique_id for incremental updates
        *
    from agg

)

select * 
from final
order by
	date_month desc,
	channel asc