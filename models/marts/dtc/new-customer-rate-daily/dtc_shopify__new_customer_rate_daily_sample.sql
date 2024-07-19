{% set sample_offer_name='sample' %}

{{ shopify__new_customer_rate_sample(
    sample_offer_name=sample_offer_name,
    shopify_orders_model='int_shopify__orders_non_recurring_sample',
    daily=True
) }}