with

recharge_orders as (

    select 
        
        id,
        hash,
        lower(note) as note,
        tags,
        lower(type) as type,
        lower(email) as email,
        lower(phone) as phone,
        charge,
        lower(status) as status,
        taxable,
        lower(currency) as currency,
        customer,
        charge_id,
        discounts,
        lower(last_name) as last_name,
        tax_lines,
        total_tax,
        address_id,
        created_at,
        lower(first_name) as first_name,
        line_items,
        updated_at,
        customer_id,
        total_price,
        processed_at,
        scheduled_at,
        shipped_date,
        lower(charge_status) as charge_status,
        total_refunds,
        discount_codes,
        shipping_lines,
        subtotal_price,
        transaction_id,
        billing_address,
        note_attributes,
        total_discounts,
        shipping_address,
        shopify_order_id, -- ALL NULL
        (external_order_id ->> 'ecommerce')::bigint as external_order_id,
        external_order_name ->> 'ecommerce' as external_order_name,
        lower(payment_processor) as payment_processor,
        shopify_cart_token,
        shopify_customer_id,
        shopify_order_number, -- ALL NULL
        (external_order_number ->> 'ecommerce')::int as external_order_number, -- number part of order_name
        total_line_items_price,

        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,

        customer ->> 'email' as email_c,
        customer ->> 'phone' as phone_c,
        customer ->> 'last_name' as last_name_c,
        customer ->> 'first_name' as first_name_c,

        created_at at time zone 'america/los_angeles' as created_at_pt,
        updated_at at time zone 'america/los_angeles' as updated_at_pt

    from {{ source('recharge', 'recharge_v2_orders') }}

)

select * from recharge_orders