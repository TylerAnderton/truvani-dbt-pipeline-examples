{% set influencer_codes=[
    'ANDREW10',
    'KARLA15',
    'JESSICA20',
    'KELSEY25'
    -- ...
] %}

{{ shopify__orders_non_recurring_discount_codes(influencer_codes) }}