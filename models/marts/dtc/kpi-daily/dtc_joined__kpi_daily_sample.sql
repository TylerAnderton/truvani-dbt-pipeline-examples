{% set spend_model='int_facebook__spend_daily_sample' %}

{% set revenue_orders_model='int_joined__revenue_orders_daily_sample' %}

{{join_spend_and_revenue_orders_main_offer_daily(
    spend_model, 
    revenue_orders_model,
    sample=True
)}}