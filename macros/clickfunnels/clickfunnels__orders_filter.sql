{% macro clickfunnels__orders_filter(
    utm_terms_exact=[],
    utm_terms_regex='',
    utm_campaigns_exact=[],
    utm_contents_exact=[]
) %}

{% set utm_terms_exact_filer=clickfunnels__utm_terms_exact_filter(utm_terms_exact) %}

{% set utm_campaigns_exact_filter=clickfunnels__utm_campaigns_exact_filter(utm_campaigns_exact) %}

{% set utm_contents_exact_filter=clickfunnels__utm_contents_exact_filter(utm_contents_exact) %}

with 

orders as (

    select 
        *,
        case
			when stripe_customer_token is not null then 1
			else null 
		end as unique_order
    from
        {{ ref('stg_clickfunnels__orders') }}
    where

        {% if utm_terms_exact|length > 0 -%}

            {{utm_terms_exact_filer}}
            
        {%- endif %}

        {% if utm_terms_regex != '' -%}

            {% if utm_terms_exact|length > 0 -%}
                or
            {%- endif %}

            utm_term ~* '{{utm_terms_regex}}'

        {%- endif %}

        {% if utm_campaigns_exact|length > 0 -%}

            {% if utm_terms_exact|length > 0 or utm_terms_regex != '' -%}
                or
            {%- endif %}

            {{utm_campaigns_exact_filter}}

        {%- endif %}

        {% if utm_contents_exact|length > 0 -%}

            {% if utm_terms_exact|length > 0 or utm_terms_regex != '' or utm_campaigns_exact|length > 0 -%}
                or
            {%- endif %}

            {{utm_contents_exact_filter}}

        {%- endif %}

)

select *
from orders
order by created_at_pst desc
    
{% endmacro %}