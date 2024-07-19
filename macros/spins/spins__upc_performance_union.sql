{% macro spins__upc_performance_union(
    time_periods,
    retail_upc_agg_models
) %}

-- {{ config(materialized='table') }}

with 

unified_data as (

    {% for i in range(retail_upc_agg_models|length) -%}
        select

            *,
            {{time_periods[i]}} as time_period_length_wks 

        from {{ ref(retail_upc_agg_models[i]) }}

        {% if not loop.last -%}
            union
        {%- endif %}
    {% endfor %}

),

previous_period_data as (

    select

        u.*,
        p.dollars as prev_dollars,
        p.units as prev_units,
        p.arp as prev_arp,
        p.avg_p_acv as prev_avg_p_acv,
        p.max_p_acv as prev_max_p_acv,
        p.num_stores_selling as prev_num_stores_selling,
        p.avg_wkly_dollars_per_store_selling as prev_avg_wkly_dollars_per_store_selling,
        p.avg_wkly_units_per_store_selling as prev_avg_wkly_units_per_store_selling
        
    from unified_data u
    
    left join unified_data p 
        on
            u.channel_outlet = p.channel_outlet and
            u.geography = p.geography and
            u.department = p.department and
            u.category = p.category and
            u.brand = p.brand and
            u.upc = p.upc and
            u.description = p.description and
            u.time_period_length_wks = p.time_period_length_wks and
            u.time_period_end_date - (u.time_period_length_wks * 7) = p.time_period_end_date

)

select * 
from previous_period_data
    
{% endmacro %}