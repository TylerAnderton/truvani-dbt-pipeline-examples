{% set agg_time_period_wks=26 %}
{% set agg_model='retail_spins__retailer_upc_performance_union' %}

{{ spins__brand_performance_aggregate(
    agg_model,
    agg_time_period_wks
)}}