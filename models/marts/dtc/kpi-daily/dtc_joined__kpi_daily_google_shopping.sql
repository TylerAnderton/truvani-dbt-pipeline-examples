{% set spend_model='int_google_ads__spend_daily_google_shopping' %}

{% set revenue_orders_model='int_joined__revenue_orders_daily_google_shopping' %}

{{join_spend_and_revenue_orders_main_offer_daily(spend_model, revenue_orders_model)}}