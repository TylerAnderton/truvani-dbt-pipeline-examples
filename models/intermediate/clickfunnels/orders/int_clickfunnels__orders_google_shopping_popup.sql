{% set utm_terms_exact=[
    'google-4-shopping-popup',
    'google-6-shopping-popup',
    'google-8-shopping-popup',
    'google-9-shopping-popup'
] %}

{{ clickfunnels__orders_filter(utm_terms_exact) }}