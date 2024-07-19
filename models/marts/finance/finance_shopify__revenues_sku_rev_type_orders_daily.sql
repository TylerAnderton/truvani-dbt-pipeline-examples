{% set line_items_model='int_shopify__orders_line_items_refunds' %}
{% set datetime_field='first_fulfillment_date' %}
{% set b2b=false %}

{{ shopify__revenues_sku_rev_type_daily(
    line_items_model,
    datetime_field,
    b2b
)}}