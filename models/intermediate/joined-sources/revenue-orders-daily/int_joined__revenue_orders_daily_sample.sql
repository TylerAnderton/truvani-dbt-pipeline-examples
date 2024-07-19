{% set shopify_model='int_shopify__revenue_orders_daily_sample' %}

{% set cf_model='int_clickfunnels__revenue_orders_daily_sample' %}

{{ join_shopify_and_cf_sample_revenue_orders_daily(shopify_model, cf_model) }}