{% set line_items_model='int_shopify__fulfillments_line_items_labeled' %}
{% set datetime_field='fulfilled_at_pt' %}
{% set b2b=false %}

{{ shopify__quantities_sku_rev_type_daily(
    line_items_model,
    datetime_field,
    b2b
)}}