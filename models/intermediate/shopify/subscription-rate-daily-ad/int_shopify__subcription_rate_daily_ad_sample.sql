{% set orders_model='int_shopify__orders_non_recurring_sample' -%}

{%- set campaigns=utm_mapping__truvani_sample_campaigns() -%}

{{shopify__subscription_rate_ad_sample(
    orders_model=orders_model,
    campaigns=campaigns,
    daily=True
)}}