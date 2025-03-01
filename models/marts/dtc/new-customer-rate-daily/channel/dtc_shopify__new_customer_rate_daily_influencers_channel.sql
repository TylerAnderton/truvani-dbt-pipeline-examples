 -- depends_on: {{ ref('int_shopify__orders_non_recurring_influencers') }}

{% set orders_model='int_shopify__orders_non_recurring_influencers' %}

{{ shopify__new_customer_rate(
    orders_model=orders_model,
    daily=True
) }}