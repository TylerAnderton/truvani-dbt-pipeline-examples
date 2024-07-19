 -- depends_on: {{ ref('int_shopify__orders_non_recurring_other_channel') }}

{% set orders_model='int_shopify__orders_non_recurring_other_channel' %}

{{ shopify__new_customer_rate(
    orders_model=orders_model,
    daily=True
) }}