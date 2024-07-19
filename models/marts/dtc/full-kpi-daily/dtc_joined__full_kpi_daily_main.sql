{% set kpi_model='dtc_joined__kpi_daily_main' %}

{% set ncr_model='dtc_shopify__new_customer_rate_daily_main' %}

{% set subrate_model='dtc_shopify__subcription_rate_daily_main' %}

{{join_kpi_ncr_subrate_main_offer_daily(
    kpi_model=kpi_model,
    ncr_model=ncr_model,
    subrate_model=subrate_model
)}}