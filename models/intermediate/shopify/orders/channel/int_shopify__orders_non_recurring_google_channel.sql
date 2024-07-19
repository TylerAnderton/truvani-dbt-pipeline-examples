{% set regex_include_list=[
    "'(?x)
    utm:source:(google|www.google.com|Google Ads|youtube)
    |utm:medium:shopping(,|$)|utm:campaign:pmax(,|$)|promo:google-shopping(,|$)|utm:campaign:shopping(,|$|pmax)|utm:medium:pmax
    |promo:display-main-promo(,|$)
    |promo:yt-main-promo(,|$|-pp)|promo:yt-sample-promo(,|$|-pp)|utm:source:youtube(,|$)
    '"
] %}

{% set regex_exclude_list=[
    "'utm:source:bing'"
] %}

{{ shopify__orders_non_recurring_tags_regex(
    regex_include_list=regex_include_list,
    regex_exclude_list=regex_exclude_list
) }}