with

source as (

    select * from {{ source('microsoft', 'ms_ads_campaigns') }}

),

final as (

    select

        "TimePeriod" AS report_date,

        "CampaignId" as campaign_id,
        "CampaignName" as campaign_name,

        "CampaignType" as campaign_type,
        "CampaignLabels" as campaign_labels,

        "CampaignStatus" as campaign_status,

        "Spend" as spend,
        "Impressions" as impressions,
        "Clicks" as clicks,

        "AverageCpc" as cpc_avg, 
        "Ctr" as ctr,
        "AverageCpm" as cpm_avg,
        
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta
        
    from source

    where "Spend" <> 0

)

select * 
from final
order by
    report_date desc,
    campaign_name asc