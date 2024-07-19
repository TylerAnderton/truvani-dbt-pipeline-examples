{% set orders_model='int_shopify__orders_non_recurring_main' %}
{% set reactivation_interval='6 months' %}
{% set daily=True %}

{%- set campaigns=utm_mapping__truvani_main_campaigns() -%}

{{ shopify__reactivation_rate(
    orders_model=orders_model,
    reactivation_interval=reactivation_interval,
    campaigns=campaigns,
    daily=daily
) }}