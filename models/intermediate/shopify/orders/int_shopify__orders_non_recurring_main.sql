{% set regex=[
    "'(?x)
    promo:main-promo(,|$)|
    utm:campaign:main_cbo|
    utm:campaign:truvani-main-(
        ,|
        $|
        influencers|
        shop|
        icecream
    )|
    utm:campaign:truvani_main_(
        ,|
        $|
        fb|
        remarketing|
        cbo(
            ,|
            $|
            3|
            4|
            _testing
        )|
        cbo_lto(
            ,|
            $|
            _test|
        )|
        asc(
            _testing|
            _tiktok|
        )
    )
    '"
]%}

{{ shopify__orders_non_recurring_tags_regex(regex) }}