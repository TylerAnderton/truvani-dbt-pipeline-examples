{% set discount_codes=[
    'TRU20OFF',
    'PROTEIN50'
] %}

{% set sku_jsonb_array=shopify__jsonb_array_from_list(items_list=discount_codes, type='code') -%}

{% set regex_exclude=
    "'(?x)
		subscription recurring order|
		promo:(google-shopping|display-protein)|
		utm:(
			source:youtube|
			medium:(shopping|pmax)|
			campaign:(
				shopping|
				pmax|
				ShoppingPMax
			)
		)(,|$)'" 
%}

with

final as (

    select
        *
    from 
        {{ ref('stg_shopify__orders') }} 
    where
        discount_codes @> ANY({{ sku_jsonb_array }})

        and tags !~* {{regex_exclude}}
        and 

        tags !~* 'subscription recurring order'
        and cancelled_at is null
)

select * 
from final
order by created_at_pt desc