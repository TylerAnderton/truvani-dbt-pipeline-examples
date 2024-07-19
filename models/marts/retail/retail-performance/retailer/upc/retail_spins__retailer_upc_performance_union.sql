{% set time_periods=[
    '4',
    '12',
    '26', 
    '52'
] %}

{% set retail_upc_agg_models=[
    'retail_spins__retailer_upc_performance_4wk',
    'retail_spins__retailer_upc_performance_12wk',
    'retail_spins__retailer_upc_performance_26wk',
    'retail_spins__retailer_upc_performance_52wk'
] %}

{{ spins__upc_performance_union(
    time_periods,
    retail_upc_agg_models
) }}