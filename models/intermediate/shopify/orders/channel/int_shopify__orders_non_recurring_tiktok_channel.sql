{% set discount_codes=[
    'TIKTOK'
] %}

{% set sku_jsonb_array=shopify__jsonb_array_from_list(items_list=discount_codes, type='code') -%}

{% set regex_include_source="'utm:source:tiktok'" %}

{% set regex_include_tags="'promo:tiktok-(main|sample)-promo'" %}

{% set regex_exclude=
    "'utm:source(facebook|google|www.google.com|Google Ads|youtube)'" 
%}

with

shopify_orders as (

    select * from {{ ref('stg_shopify__orders') }}

),

final as (

    select
        *
    from 
        shopify_orders 
    where
        discount_codes @> ANY({{ sku_jsonb_array }})

        or tags ~* {{regex_include_source}}

        or (
			tags ~* {{regex_include_tags}}
			and tags !~* {{regex_exclude}}
		)

        and 

        tags !~* 'subscription recurring order'
        and cancelled_at is null
)

select * 
from final
order by created_at_pt desc