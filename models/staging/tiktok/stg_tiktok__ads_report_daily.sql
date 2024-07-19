with

source as (

    select * from {{ source('tiktok', 'tiktok_adsy') }}

),

final as (

    select

        ad_id,
        (metrics ->> 'ad_name') as ad_name,

        adgroup_id,
        (metrics ->> 'adgroup_name') as adgroup_name,

        campaign_id,
        (metrics ->> 'campaign_name') as campaign_name,

        stat_time_day::date as report_date,

        (metrics ->> 'spend')::float as spend,

        (metrics ->> 'reach')::int as reach,
        (metrics ->> 'impressions')::int as impressions,
        (metrics ->> 'clicks')::int as clicks,
        
        (metrics ->> 'cpc')::float as cpc,
        (metrics ->> 'cpm')::float as cpm,
        (metrics ->> 'ctr')::float as ctr,

        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta
        
    from  source       

)

select * 
from final