{% set kpi_daily_model='dtc_joined__kpi_daily_facebook_channel' %}

{{ agg_kpi_monthly(
    kpi_daily_model=kpi_daily_model,
    month_to_date=True
)}}