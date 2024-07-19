{% macro shopify__combine_ncr_adset_names(
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

		round(sum(total)::numeric, 2) as total_revenue,
		round(sum(total * new_order)::numeric, 2) as new_revenue,
		round(sum(total * returning_order)::numeric, 2) as returning_revenue,

		count(*) as total_orders,
		sum(new_order) as new_order_count,
		sum(returning_order) as returning_order_count,

		round(
            (
                sum(new_order) 
                / 
                nullif((sum(new_order) + sum(returning_order)), 0)::float
            )::numeric, 
            2
        ) as new_customer_rate

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
	  	a.new_revenue + b.new_revenue as new_revenue,
	  	a.returning_revenue + b.returning_revenue as returning_revenue,

	  	a.total_orders + b.total_orders as total_orders,
	  	a.new_order_count + b.new_order_count as new_order_count,
	  	a.returning_order_count + b.returning_order_count as returning_order_count,
	  	
	  	round(
            (
                (a.new_order_count + b.new_order_count) 
                / 
                nullif(
                    (
                        (a.new_order_count + b.new_order_count) 
                        + 
                        (a.returning_order_count + b.returning_order_count)
                    ), 
                    0
                )::float
            )::numeric,
            2
        ) as new_customer_rate
	  	
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