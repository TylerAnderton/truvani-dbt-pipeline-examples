{% set facebook_metrics_model='int_facebook__ads_metrics_daily_main' %}
{% set ncr_model='int_shopify__new_customer_rate_daily_ad_main' %}
{% set subrate_model='int_shopify__subcription_rate_daily_ad_main' %}
{% set reactivation_model='int_shopify__reactivation_rate_daily_ad_main' %}

{{ join_facebook_metrics_and_ncr_main_offer_daily(
    facebook_metrics_model,
    ncr_model,
    subrate_model=subrate_model,
    reactivation_model=reactivation_model,
    agg_level='ad'
) }}