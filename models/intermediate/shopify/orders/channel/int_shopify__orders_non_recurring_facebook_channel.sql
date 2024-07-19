{% set regex=[
    "'utm:source:facebook'",
    "'utm:medium:(cpc|cpm)'"
] %}

{{ shopify__orders_non_recurring_tags_regex(regex) }}