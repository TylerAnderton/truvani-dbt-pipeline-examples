{% macro join_kpi_ncr_subrate_main_offer_daily(
    kpi_model,
    ncr_model,
    subrate_model=None
) %}

with

kpi_daily as (

    select * from {{ ref(kpi_model) }}

),

ncr_daily as (

    select * from {{ ref(ncr_model) }}

),

{% if subrate_model -%}
subrate_daily as (

    select * from {{ ref(subrate_model) }}

),
{%- endif %}


final as (

    select
        coalesce(
            kpi_daily.date_,
            ncr_daily.date_pst
            {% if subrate_model -%}
                ,
            subrate_daily.date_pst
            {%- endif %}
        ) as date_pst,

        coalesce(kpi_daily.spend, 0) as spend,
        coalesce(kpi_daily.standard_revenue, 0) as standard_revenue,
        coalesce(kpi_daily.standard_order_count, 0) as standard_order_count,
        coalesce(kpi_daily.popup_revenue, 0) as popup_revenue,
        coalesce(kpi_daily.popup_order_count, 0) as popup_order_count,

        coalesce(
            kpi_daily.total_revenue,
            ncr_daily.total_revenue,
            {% if subrate_model -%}
                subrate_daily.total_revenue,
            {%- endif %}
            0
        ) as total_revenue,

        coalesce(
            kpi_daily.total_order_count,
            ncr_daily.total_orders,
            {% if subrate_model -%}
                subrate_daily.order_count,
            {%- endif %}            
            0
        ) as total_order_count,

        coalesce(ncr_daily.new_revenue, 0) as new_revenue,
        coalesce(ncr_daily.returning_revenue, 0) as returning_revenue,

        coalesce(ncr_daily.new_order_count, 0) as new_order_count,
        coalesce(ncr_daily.returning_order_count, 0) as returning_order_count,

        {% if subrate_model -%}
        coalesce(subrate_daily.subscription_revenue, 0) as subscription_revenue,
        coalesce(subrate_daily.otp_revenue, 0) as otp_revenue,

        coalesce(subrate_daily.subscription_count, 0) as subscription_count,
        coalesce(subrate_daily.otp_count, 0) as otp_count,
        {%- endif %}

        kpi_daily.roas,
        kpi_daily.aov,
        kpi_daily.cpa,

        ncr_daily.new_customer_rate

        {% if subrate_model -%}
            ,
            subrate_daily.subscription_rate
        {%- endif %}     
        
    
    from kpi_daily
    
    full join ncr_daily
        on kpi_daily.date_ = ncr_daily.date_pst

    {% if subrate_model -%}
    full join subrate_daily
        on kpi_daily.date_ = subrate_daily.date_pst    
    {%- endif %}
    

)

select * 
from final
order by date_pst desc
    
{% endmacro %}