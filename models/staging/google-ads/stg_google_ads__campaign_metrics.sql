with

source as (

    select 

        segments_date as report_date,

        campaign_id,
        campaign_name,
        campaign_advertising_channel_type as campaign_type,
        campaign_advertising_channel_sub_type as campaign_subtype,

        metrics_clicks as clicks,
        metrics_cost_micros::float/1000000 as spend,
        metrics_impressions as impressions,

        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta

    from {{ source('google_ads', 'google_ads_campaigns') }}

)

select * 
from source
order by 
    report_date desc,
    campaign_name asc