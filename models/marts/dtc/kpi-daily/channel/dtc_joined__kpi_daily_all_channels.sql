{% set spend_model='int_joined__spend_daily_all_channels' %}
{% set ncr_model='dtc_shopify__new_customer_rate_daily_all_channels' %}

{{ join_spend_ncr_daily(
    spend_model=spend_model,
    ncr_model=ncr_model
)}}