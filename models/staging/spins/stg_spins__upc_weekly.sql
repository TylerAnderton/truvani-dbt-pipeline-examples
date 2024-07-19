with

spins_upc_weekly as (

    select distinct

        "Channel/Outlet" as channel_outlet,
        "Geography Level" as geography_level,
        "Geography" as geography,
        "Time Period" as time_period_length,
        to_date("Time Period End Date", 'MM/DD/YYYY') as time_period_end_date,
        "Product Universe" as product_universe,
        "Product Level" as product_level,
        "Department" as department,
        "Category" as category,
        "Subcategory" as subcategory,
        "Brand" as brand,
        "UPC" as upc,
        "Description" as description,
        to_date("First Week Selling", 'MM/DD/YYYY') as first_week_selling,
        ((to_date("Time Period End Date", 'MM/DD/YYYY') - to_date("First Week Selling", 'MM/DD/YYYY')) / 7)::int as product_lifetime_wks,

        nullif("Dollars", '')::float as dollars,
        nullif("Units", '')::float as units,

        nullif("Avg % ACV", '')::float/100 as avg_p_acv,
        nullif("Max % ACV", '')::float/100 as max_p_acv,
        nullif("TDP", '')::float as tdp,

        nullif("# of Stores", '')::int as num_stores,
        nullif("# of Stores Selling", '')::int as num_stores_selling,
        nullif("% of Stores Selling", '')::float as p_stores_selling,

        nullif("Average Weekly Dollars per Store Selling", '')::float as avg_wkly_dollars_per_store_selling,
        nullif("Average Weekly Dollars Per Store Selling Per Item", '')::float as avg_wkly_dollars_per_store_selling_per_item,
        nullif("Average Weekly Dollars", '')::float as avg_wkly_dollars,

        nullif("Average Weekly Units per Store Selling", '')::float as avg_wkly_units_per_store_selling,
        nullif("Average Weekly Units Per Store Selling Per Item", '')::float as avg_wkly_units_per_store_selling_per_item,
        nullif("Average Weekly Units", '')::float as avg_wkly_units,

        nullif("Dollars Per Store Selling Per Item", '')::float as dollars_per_store_selling_per_item,
        nullif("Units Per Store Selling Per Item", '')::float as units_per_store_selling_per_item

    from {{ source('spins', 'spins_retail_upc_weekly') }}

)

select * 
from spins_upc_weekly