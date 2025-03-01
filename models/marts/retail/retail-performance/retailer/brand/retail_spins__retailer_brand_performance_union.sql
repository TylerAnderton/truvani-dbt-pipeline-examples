{{ config(
    materialized='table',
    on_schema_change='sync',
    post_hook="
        ALTER TABLE retail_spins__retailer_brand_performance_union RENAME TO retail_spins__retailer_brand_performance_union_old;
        ALTER TABLE retail_spins__retailer_brand_performance_union_stg RENAME TO retail_spins__retailer_brand_performance_union;
        DROP TABLE retail_spins__retailer_brand_performance_union_old;
    "
) }}

select *
from {{ ref('retail_spins__retailer_brand_performance_union_stg') }}