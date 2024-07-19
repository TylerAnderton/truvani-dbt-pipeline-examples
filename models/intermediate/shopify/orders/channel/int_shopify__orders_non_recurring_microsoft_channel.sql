{% set regex=[
    "'utm:source:bing'",
    "'utm:medium:(cpc|cpm)'"
] %}

{{ shopify__orders_non_recurring_tags_regex(regex) }}