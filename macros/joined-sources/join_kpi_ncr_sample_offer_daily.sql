{% macro join_kpi_ncr_sample_offer_daily(
    kpi_model,
    ncr_model
) %}

{{ config(
    materialized='incremental',
    unique_key='date_pst',
    on_schema_change='sync'
) }}

with

kpi_daily as (

    select * from {{ ref(kpi_model) }}

),

ncr_daily as (

    select * from {{ ref(ncr_model) }}

),

final as (

    select
        coalesce(
            kpi_daily.date_,
            ncr_daily.date_pst
        ) as date_pst,

        coalesce(kpi_daily.spend, 0) as spend,

        coalesce(
            kpi_daily.revenue,
            ncr_daily.total_revenue,
            0
        ) as total_revenue,

        coalesce(
            kpi_daily.order_count,
            ncr_daily.total_orders,
            0
        ) as total_order_count,

        coalesce(ncr_daily.new_revenue, 0) as new_revenue,
        coalesce(ncr_daily.returning_revenue, 0) as returning_revenue,

        coalesce(ncr_daily.new_order_count, 0) as new_order_count,
        coalesce(ncr_daily.returning_order_count, 0) as returning_order_count,

        kpi_daily.roas,
        kpi_daily.aov,
        kpi_daily.cpa,

        ncr_daily.new_customer_rate
    
    from kpi_daily
    full join ncr_daily
        on kpi_daily.date_ = ncr_daily.date_pst

)

select * 
from final
order by date_pst desc
    
{% endmacro %}