{% set regex=[
    "'(?x)
		utm:term:sample-pack
    '"
]%}

{{ shopify__orders_non_recurring_tags_regex(regex) }}