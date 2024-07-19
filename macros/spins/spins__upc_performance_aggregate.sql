{% macro spins__upc_performance_aggregate(
    agg_time_period_wks,
    geography_level
) %}

with 

upc_weekly as (

    select * 
    from {{ ref('int_spins__upc_performance_weekly') }}
    where geography_level = {{geography_level}}
    {% if geography_level == "'KEY ACCOUNT'" -%}
        and geography ~* 'TOTAL US'
    {%- endif %}

),

initial_agg as (

    select

        channel_outlet,
        geography,
        time_period_end_date,
        department,
        category,
        brand,
        upc,
        description,

        sum(dollars) over agg_window as dollars,
        sum(units) over agg_window as units,

        (sum(dollars) over agg_window)/nullif((sum(units) over agg_window), 0)  as arp,
        
        avg(avg_p_acv) over agg_window as avg_p_acv,
        max(max_p_acv) over agg_window as max_p_acv,

        max(num_stores_selling) over agg_window as num_stores_selling,

        (sum(dollars) over agg_window)/{{agg_time_period_wks}} as avg_wkly_dollars,
        (sum(units) over agg_window)/{{agg_time_period_wks}} as avg_wkly_units

    from upc_weekly

    window agg_window as (

        partition by 

            channel_outlet,
            geography,
            department,
            category,
            brand,
            upc,
            description

        order by time_period_end_date
        rows between {{agg_time_period_wks - 1}} preceding and current row
    )

),

calc_metrics as (

    select

        channel_outlet,
        geography,
        time_period_end_date,
        department,
        category,
        brand,
        upc,
        description,

        round(dollars::numeric, 2) as dollars,
        round(units::numeric, 2) as units,
        round(arp::numeric, 2) as arp,
        round(avg_p_acv::numeric, 2) as avg_p_acv,
        round(max_p_acv::numeric, 2) as max_p_acv,
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
    brand asc,
    description asc
    
{% endmacro %}