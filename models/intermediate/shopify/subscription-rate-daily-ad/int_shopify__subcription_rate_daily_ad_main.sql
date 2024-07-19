{% set orders_model='int_shopify__orders_non_recurring_main' -%}

{%- set campaigns=utm_mapping__truvani_main_campaigns() -%}

{{shopify__subscription_rate_ad(
    orders_model=orders_model,
    campaigns=campaigns,
    daily=True
)}}