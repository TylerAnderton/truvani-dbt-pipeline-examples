with

shopify_customers as (

    select
        
        id,
        note,
        lower(tags) as tags,
        lower(email) as email,
        phone,
        lower(state) as state,
        addresses,
        lower(last_name) as last_name,
        created_at,
        lower(first_name) as first_name,
        tax_exempt,
        updated_at,
        total_spent,
        orders_count,
        last_order_id,
        tax_exemptions,
        verified_email,
        default_address,
        last_order_name,
        accepts_marketing,
        sms_marketing_consent,
        marketing_opt_in_level,
        email_marketing_consent,
        accepts_marketing_updated_at,

        -- _airbyte_shopify_customers_hashid,
        -- _airbyte_unique_key,
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,

        created_at at time zone 'america/los_angeles' as created_at_pt,
        updated_at at time zone 'america/los_angeles' as updated_at_pt
        
    from {{ source('shopify', 'shopify_v2_customers') }}

)

select * from shopify_customers