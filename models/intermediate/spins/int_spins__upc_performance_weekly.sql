with 

upc_weekly as (

    select

        case 
            when channel_outlet = 'NATURAL EXPANDED' then 'NATURAL'
            else channel_outlet
        end as channel_outlet,

        geography_level,

        case
            when geography = 'TOTAL US - NATURAL EXPANDED CHANNEL' then 'TOTAL US - NATURAL CHANNEL'
            else geography
        end as geography,

        time_period_end_date,
        department,
        category,
        brand,
        upc,
        description,
        first_week_selling,
        product_lifetime_wks,

        dollars,
        units,
        avg_p_acv,
        max_p_acv,
        tdp,
        num_stores,
        num_stores_selling,
        p_stores_selling,
        avg_wkly_dollars_per_store_selling,
        avg_wkly_dollars,
        avg_wkly_units_per_store_selling,
        avg_wkly_units

    from {{ ref('stg_spins__upc_weekly') }}

)

select *
from upc_weekly
order by
        time_period_end_date desc,
        geography asc,
        department asc,
        category asc,
        brand asc,
        description asc