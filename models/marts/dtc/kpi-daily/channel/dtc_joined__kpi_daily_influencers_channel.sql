{% set spend_model='int_influencers__spend_daily_channel' %}
{% set ncr_model='dtc_shopify__new_customer_rate_daily_influencers_channel' %}

{{ join_spend_ncr_daily(
    spend_model=spend_model,
    ncr_model=ncr_model
)}}