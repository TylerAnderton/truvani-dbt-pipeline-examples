{% set agg_time_period_wks=52 %}
{% set geography_level="'TOTAL US'" %}

{{ spins__upc_performance_aggregate(
    agg_time_period_wks,
    geography_level
)}}