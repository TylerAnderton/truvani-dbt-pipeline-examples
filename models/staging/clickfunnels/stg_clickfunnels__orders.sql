with 

clickfunnels_orders as (

    select 

        charge_id,
        created_at_pst,
        lower(first_name) as first_name,
        lower(last_name) as last_name,
        lower(email) as email,
        phone,
        lower(shipping_address) as shipping_address,
        lower(shipping_city) as shipping_city,
        lower(shipping_state) as shipping_state,
        lower(shipping_country) as shipping_country,
        shipping_zip_code,
        product_ids,
        lower(product_names) as product_names,
        funnel_id,
        funnel_step_id,
        subtotal,
        total,
        lower(currency) as currency,
        lower(status) as status,
        subscription_id,
        lower(utm_source) as utm_source,
        lower(utm_medium) as utm_medium,
        lower(utm_campaign) as utm_campaign,
        lower(utm_term) as utm_term,
        lower(utm_content) as utm_content,
        stripe_customer_token

    from {{ source('clickfunnels', 'clickfunnel_orders') }}

)

select * from clickfunnels_orders