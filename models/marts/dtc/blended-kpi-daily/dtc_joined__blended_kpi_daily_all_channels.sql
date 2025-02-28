{% set spend_model='int_joined__spend_daily_all_channels' %}
{% set sub_ncr_model='int_shopify__subscription_ncr_daily_all_channels' %}

{{ join_blended_kpi_daily(
    spend_model=spend_model,
    sub_ncr_model=sub_ncr_model
)}}