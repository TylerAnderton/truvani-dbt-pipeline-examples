{% set main_offer_name='main' -%}

{%- set campaigns=utm_mapping__truvani_main_campaigns() -%}

{{ shopify__new_customer_rate_ad(
    main_offer_name=main_offer_name,
    campaigns=campaigns,
    daily=True
) }}