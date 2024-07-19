{% set utm_terms_exact=[
    'main-popup',
    'main-popup-followup',
    'main-popup-vc',
    'main-popup-vc-followup'
] %}

{{ clickfunnels__orders_filter(utm_terms_exact) }}