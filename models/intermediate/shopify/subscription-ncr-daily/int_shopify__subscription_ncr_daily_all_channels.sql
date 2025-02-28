{% set orders_model='stg_shopify__orders' %}

{{ shopify__subscription_ncr(
    orders_model=orders_model,
    ads_regex=all_ads_tags_regex(),
    daily=True
) }}