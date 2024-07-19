{% set agg_time_period_wks=4 %}
{% set geography_level="'KEY ACCOUNT'" %}

{{ spins__upc_performance_aggregate(
    agg_time_period_wks,
    geography_level
)}}