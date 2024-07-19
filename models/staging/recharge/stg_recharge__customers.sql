with

recharge_customers as (

    select 
        
        id,
        hash,
        lower(email) as email,
        lower(phone) as phone,
        lower(status) as status,
        lower(last_name) as last_name,
        created_at,
        lower(first_name) as first_name,
        tax_exempt,
        updated_at,
        billing_zip,
        lower(billing_city) as billing_city,
        billing_phone,
        analytics_data,
        lower(processor_type) as processor_type,
        lower(billing_company) as billing_company,
        lower(billing_country) as billing_country,
        lower(billing_address1) as billing_address1,
        lower(billing_address2) as billing_address2,
        lower(billing_province) as billing_province,
        accepts_marketing,
        lower(billing_last_name) as billing_last_name,
        lower(billing_first_name) as billing_first_name,
        shopify_customer_id, -- ALL NULL
        (external_customer_id ->> 'ecommerce')::bigint as external_customer_id,
        number_subscriptions,
        stripe_customer_token,
        has_valid_payment_method,
        first_charge_processed_at,
        has_card_error_in_dunning,
        subscriptions_total_count,
        subscriptions_active_count,
        number_active_subscriptions, -- ALL NULL
        lower(reason_payment_method_not_valid) as reason_payment_method_not_valid,
        
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,

        created_at at time zone 'america/los_angeles' as created_at_pt,
        updated_at at time zone 'america/los_angeles' as updated_at_pt,
        first_charge_processed_at at time zone 'america/los_angeles' as first_charge_processed_at_pt

    from {{ source('recharge', 'recharge_v2_customers') }}

)

select * from recharge_customers