{% set sample_offer_name='sample' -%}

{%- set campaigns=utm_mapping__truvani_sample_campaigns() -%}

{{ shopify__new_customer_rate_ad_sample(
    sample_offer_name=sample_offer_name,
    campaigns=campaigns,
    daily=True
) }}