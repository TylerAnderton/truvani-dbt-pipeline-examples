{% set time_periods=[
    '4',
    '12',
    '26', 
    '52'
] %}

{% set retail_brand_agg_models=[
    'retail_spins__brand_performance_4wk',
    'retail_spins__brand_performance_12wk',
    'retail_spins__brand_performance_26wk',
    'retail_spins__brand_performance_52wk'
] %}

{{ spins__brand_performance_union(
    time_periods,
    retail_brand_agg_models
) }}