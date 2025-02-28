{% set regex_include_list=[
    "'(?x)
		utm:medium:shopping(,|$)
        |promo:google-shopping(,|$)
        |promo:google-nonbranded-shopping(,|$)
        |utm:campaign:shopping(,|$)
        |utm:campaign:standardshopping(,|$|-br|-nb)
    '"
]%}

{% set regex_exclude_list=[
    "'utm:source:bing'"
] %}

{% set discount_codes=[
    'GOOGLE25',
    'SHOPPING50'
] %}

{% set code_regex_include_list=[
    '^BR.*',
    '^NB.*'
] %}

select * from (
    {{ shopify__orders_non_recurring_tags_regex(
        regex_include_list=regex_include_list,
        regex_exclude_list=regex_exclude_list
    ) }}
) as regex_orders

union

select * from (
    {{ shopify__orders_non_recurring_discount_codes(discount_codes) }}
) as discount_code_orders

union

select * from (
    {{ shopify__orders_non_recurring_discount_code_regex(
        regex_include_list=code_regex_include_list
    ) }}
) as discount_regex_orders