{% set kpi_model='dtc_joined__kpi_daily_sample' %}

{% set ncr_model='dtc_shopify__new_customer_rate_daily_sample' %}

{{join_kpi_ncr_sample_offer_daily(
    kpi_model=kpi_model,
    ncr_model=ncr_model
)}}