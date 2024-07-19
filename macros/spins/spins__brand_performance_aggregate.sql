{% macro spins__brand_performance_aggregate(
    agg_model,
    agg_time_period_wks
) %}

with 

upc_agg as (

    select * 
    from {{ ref(agg_model) }}
    where time_period_length_wks = {{agg_time_period_wks}}

),

initial_agg as (

    select

        channel_outlet,
        geography,
        time_period_end_date,
        department,
        category,
        brand,

        sum(dollars) as dollars,
        sum(units) as units,

        sum(dollars)/nullif(sum(units), 0)  as arp,
        
        sum(avg_p_acv)/{{agg_time_period_wks}} as avg_p_acv,
        sum(max_p_acv) as tdp,

        max(num_stores_selling) as num_stores_selling,

        (sum(dollars))/{{agg_time_period_wks}} as avg_wkly_dollars,
        (sum(units))/{{agg_time_period_wks}} as avg_wkly_units

    from upc_agg
    group by
        channel_outlet,
        geography,
        time_period_end_date,
        department,
        category,
        brand

),

calc_metrics as (

    select

        channel_outlet,
        geography,
        time_period_end_date,
        department,
        category,
        brand,

        round(dollars::numeric, 2) as dollars,
        round(units::numeric, 2) as units,
        round(arp::numeric, 2) as arp,
        round(avg_p_acv::numeric, 2) as avg_p_acv,
        round(tdp::numeric, 2) as tdp,
        num_stores_selling,

        round((avg_wkly_dollars/nullif(num_stores_selling, 0))::numeric, 2) as avg_wkly_dollars_per_store_selling,
        round((avg_wkly_units/nullif(num_stores_selling, 0))::numeric, 2) as avg_wkly_units_per_store_selling

    from initial_agg

)

select *
from calc_metrics
order by
    time_period_end_date desc,
    geography asc,
    department asc,
    category asc,
    brand asc
    
{% endmacro %}