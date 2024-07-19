{% set shopify_model='int_shopify__revenue_orders_daily_google_shopping' %}

{% set popup_model='int_clickfunnels__revenue_orders_daily_google_shopping_popup' %}

{{ join_shopify_and_popup_revenue_orders_daily(shopify_model, popup_model) }}