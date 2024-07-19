{% set campaign_types=[
    'shopping'
] %}

{{ google_ads__spend(
    campaign_types=campaign_types,
    daily=true
)}}