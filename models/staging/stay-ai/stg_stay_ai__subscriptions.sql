with

stay_ai_subscriptions as (

    select distinct

        "id",
        substring("subscriptionId" from '[0-9]+$')::bigint as subscription_id,
        lower("status") as status,
        "orderIntervalFrequency" as order_interval_freq,
        lower("orderIntervalUnit") as order_interval_unit,

        "customerId" as customer_id,
        lower("lastName") as last_name,
        lower("firstName") as first_name,
        lower("emailAddress") as email,
        "deliveryAddress" as address,

        date_trunc('second', to_timestamp("createdAt", 'yyyy-mm-dd"t"hh24:mi:ss.ms"z"') at time zone 'utc')::timestamptz as created_at,
        date_trunc('second', to_timestamp("updatedAt", 'yyyy-mm-dd"t"hh24:mi:ss.ms"z"') at time zone 'utc')::timestamptz as updated_at,
        date_trunc('second', to_timestamp("churnedAt", 'yyyy-mm-dd"t"hh24:mi:ss.ms"z"') at time zone 'utc')::timestamptz as churned_at,
        date_trunc('second', to_timestamp("cancelledAt", 'yyyy-mm-dd"t"hh24:mi:ss.ms"z"') at time zone 'utc')::timestamptz as cancelled_at,
        date_trunc('second', to_timestamp("pausedUntil", 'yyyy-mm-dd"t"hh24:mi:ss.ms"z"') at time zone 'utc')::timestamptz as paused_until,
        date_trunc('second', to_timestamp("lastChargeDate", 'yyyy-mm-dd"t"hh24:mi:ss.ms"z"') at time zone 'utc')::timestamptz as last_charge_dt,
        date_trunc('second', to_timestamp("nextBillingDate", 'yyyy-mm-dd"t"hh24:mi:ss.ms"z"') at time zone 'utc')::timestamptz as next_billing_dt,

        "price",
        "deliveryPrice" as shipping_price,

        "lineItems" as line_items,
        "cancellationReason" as cancel_reason,

        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta
        
    from {{ source('stay_ai', 'stayai_v2_Subscriptions') }}

),

stay_ai_subscriptions_trans as (

    select 

        *,

        created_at at time zone 'america/los_angeles' as created_at_pt,
        updated_at at time zone 'america/los_angeles' as updated_at_pt,
        churned_at at time zone 'america/los_angeles' as churned_at_pt,
        cancelled_at at time zone 'america/los_angeles' as cancelled_at_pt,
        paused_until at time zone 'america/los_angeles' as paused_until_pt,
        last_charge_dt at time zone 'america/los_angeles' as last_charge_dt_pt,
        next_billing_dt at time zone 'america/los_angeles' as next_billing_dt_pt

    from stay_ai_subscriptions

)

select * from stay_ai_subscriptions_trans