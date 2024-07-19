with

recharge_subscriptions as (

    select 
        
        id,
        sku,
        lower(email) as email,
        price,
        lower(status) as status,
        quantity,
        created_at,
        properties,
        updated_at,
        customer_id::bigint,
        cancelled_at,
        product_title,
        variant_title,
        analytics_data,
        has_queued_charges,
        order_day_of_month,
        shopify_product_id,
        shopify_variant_id,
        cancellation_reason,
        (external_product_id ->> 'ecommerce')::bigint as external_product_id,
        (external_variant_id ->> 'ecommerce')::bigint as external_variant_id,
        max_retries_reached,
        order_interval_unit,
        recharge_product_id,
        next_charge_scheduled_at,
        order_interval_frequency,
        charge_interval_frequency,
        cancellation_reason_comments,
        expire_after_specific_number_of_charges,

        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,

        created_at at time zone 'america/los_angeles' as created_at_pt,
        updated_at at time zone 'america/los_angeles' as updated_at_pt,
        cancelled_at at time zone 'america/los_angeles' as cancelled_at_pt
        
    from {{ source('recharge', 'recharge_v2_subscriptions') }}

)

select * from recharge_subscriptions