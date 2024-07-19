{% set utm_terms_exact=[
    'sample-pack',
    'sample-pack-cold-followup'
] %}

{{ clickfunnels__orders_filter(utm_terms_exact) }}