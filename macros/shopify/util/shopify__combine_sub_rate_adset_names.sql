{% macro shopify__combine_sub_rate_adset_names(
    daily=False
) -%}
    
initial_agg as (

	select
		{% if daily -%}
            date_pst,
        {% endif -%}

		campaign_name,
		adset_name,
		ad_name,

		round(
            sum(total_price)::numeric,
            2
        ) as total_revenue,

        round(
            sum(total_price * subscription)::numeric, 2
        ) as subscription_revenue,

        round(
            (
                sum(total_price)::numeric
                - 
                sum(total_price * subscription)::numeric
            ),
            2
        ) as otp_revenue,

        count(*) as order_count,

        sum(subscription) as subscription_count,

        count(*) - sum(subscription) as otp_count,
        
        round(
            (
                sum(subscription)
                /
                count(*)::float
            )::numeric,
            2
        ) as subscription_rate

	from orders_labeled

	group by
		{% if daily -%}
            date_pst,
        {% endif -%}

		campaign_name,
		adset_name,
		ad_name

),

matched_adsets as (

	select
	  	{% if daily -%}
            a.date_pst,
        {% endif -%}
	  	a.campaign_name,

	  	case
	    	when a.adset_name like '%' || b.adset_name || '%' then a.adset_name
	    	else b.adset_name
	  	end as adset_name,

	  	a.ad_name,

	  	a.total_revenue + b.total_revenue as total_revenue,
	  	a.subscription_revenue + b.subscription_revenue as subscription_revenue,
	  	a.otp_revenue + b.otp_revenue as otp_revenue,

	  	a.order_count + b.order_count as order_count,
	  	a.subscription_count + b.subscription_count as subscription_count,
	  	a.otp_count + b.otp_count as otp_count,
	  	
	  	round(
            (
                (a.subscription_count + b.subscription_count) 
                / 
                nullif(
                    (a.order_count + b.order_count), 
                    0
                )::float
            )::numeric,
            2
        ) as subscription_rate
	  	
	from initial_agg a

	join initial_agg b
        on
            {% if daily -%}
                a.date_pst = b.date_pst and
            {% endif -%}
            
            a.campaign_name = b.campaign_name
            and a.ad_name = b.ad_name
            and a.adset_name like '%' || b.adset_name || '%'
	
    where a.adset_name <> b.adset_name
),

final_union as (

    select * 
    from matched_adsets

    union

    select *
    from initial_agg
    where not exists (
        select 1
        from matched_adsets
        where 
            (
                matched_adsets.adset_name like '%' || initial_agg.adset_name || '%'
                or initial_agg.adset_name like '%' || matched_adsets.adset_name || '%'
            )
            {% if daily -%}
                and initial_agg.date_pst = matched_adsets.date_pst
            {% endif -%}
            
            and initial_agg.campaign_name = matched_adsets.campaign_name
            and initial_agg.ad_name = matched_adsets.ad_name
    )

)

{%- endmacro %}