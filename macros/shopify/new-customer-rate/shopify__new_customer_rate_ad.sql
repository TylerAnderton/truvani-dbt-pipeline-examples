{% macro shopify__new_customer_rate_ad(
    main_offer_name,
    campaigns,
    daily=False,
    start_date=None,
    end_date=None
) -%}

{% set orders_model='int_shopify__orders_non_recurring_' ~ main_offer_name -%}

with 

orders as (

    select * from {{ ref(orders_model) }}

),

orders_labeled_ as (

	select
        {% if daily -%}
            created_at_pt::date as date_pst,
        {% endif -%}
		
		case
            {% for tag, campaign_name in campaigns.items() -%}
                when tags ~* 'utm:campaign:{{tag}}' then '{{campaign_name}}'    
            {% endfor -%}
			else (regexp_match(tags, 'utm:campaign:([^,]*)')) [1] -- 'No campaign attribution'
		end as campaign_name, -- manually enter facebook campaign names
		
		(regexp_match(tags, 'utm:term:([^,]*)')) [1] as adset_name,
		(regexp_match(tags, 'utm:content:([^,]*)')) [1] as ad_name,
		
		case when tags ~* 'new customer' then
			1
		else
			0
		end as new_order,
		
		case when tags ~* 'returning customer' then
			1
		else
			0
		end as returning_order,
		
		total_price as total
		
	from
		orders
	
	-- where
		-- tags ~* '(?x)
		-- 	utm:campaign:(
        --         {%- for tag in campaigns %}
        --             {{tag}}
        --             {%- if not loop.last -%}
        --                 |
        --             {%- endif -%}
        --         {%- endfor %}
		-- 	)'
        
        {% if start_date and end_date -%}
           where created_at_pt::date between make_date({{start_date}}) and make_date({{end_date}}) 
        {%- endif %}

), 

-- MAP OLD AD/ADSET NAMES TO NEW
{% set ad_name_map={
    'Protein50 Welcome Kit':'P50 Welcome - PC1',
    'Vchai - Image 1 - 1/16/24':'Vchai - I1-C1',
    'Vchai - Image 2 - 1/16/24':'Vchai - I2-C1',
    'Vchai - Image 3 - 1/16/24':'Vchai - I3-C1',
    'Mel Video 11/24/21 - Melanie':'Mel Video 11/24/21 - Melanie',
    'New Black Shaker ASMR':'NEW ASMR - PC1',
    'Black Shaker Brownies 1':'Brownies I1 - PC1',
    'Black Shaker Brownies 2':'Brownies I2 - PC1',
    'Black Shaker Brownies 3':'Brownies I3 - PC1',
    'Shaker Starter Kit Ad 2 - Co':'SHAKER ASMR - PC2',
    'DCT New Shaker 7/25':'DCT New 7/25 - PC1',
    'Pink Shaker Welcome Kit':'Pink Welcome - PC1',
    'Pink Shaker Chocolate Cake 2':'Pink Cake - PC1',
    'Winning ABO Seasonal Pink Sh':'Winning Pink - PI1-PC1',
    'Winning ABO Seasonal Pink Sh':'Mel Pink - PI1-PC1',
    'Mel Video 11/24/21 - Melanie':'Mel Video 11/24/21 - Melanie',
    'Winning Pink Shaker Ad - Kay':'Kayla Pink - PI1-PC1'
} %}

{% set adset_name_map={
    'Broad - F - US - 25-65 - New Ye':'Broad - F - US - 25-65 - 1/16',
    'Broad - F - US - 25-65 - Season':'Broad - F - US - 25-65 - Season'
} %}

orders_labeled as (

    select

        {% if daily -%}
            date_pst,
        {% endif -%}

        campaign_name,

        case
            {% for old_name, new_name in adset_name_map.items() -%}
                when adset_name = '{{old_name}}' then '{{new_name}}'
            {% endfor -%}
			else adset_name
		end as adset_name,

        case
            {% for old_name, new_name in ad_name_map.items() -%}
                when ad_name = '{{old_name}}' then '{{new_name}}'
            {% endfor -%}
			else ad_name
		end as ad_name,

        new_order,
        returning_order,
        total
    
    from orders_labeled_

),

{{ shopify__combine_ncr_adset_names(daily) }}

select *
from final_union
order by
    {% if daily -%}
        date_pst desc,
    {%- endif %}

  	campaign_name asc,
  	adset_name asc,
  	ad_name asc
    
{% endmacro %}