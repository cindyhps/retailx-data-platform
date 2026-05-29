{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

-- Purpose: Clean and transform raw orders data, ensuring correct data types and filtering out invalid records

with source as (
    select * from {{ source('raw', 'orders') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        order_status,

        -- cast timestamp columns, handle empty strings
        case when order_purchase_timestamp = '' then null
             else cast(order_purchase_timestamp as timestamp) end     as ordered_at,
        case when order_approved_at = '' then null
             else cast(order_approved_at as timestamp) end            as approved_at,
        case when order_delivered_carrier_date = '' then null
             else cast(order_delivered_carrier_date as timestamp) end as delivered_carrier_at,
        case when order_delivered_customer_date = '' then null
             else cast(order_delivered_customer_date as timestamp) end as delivered_customer_at,
        case when order_estimated_delivery_date = '' then null
             else cast(order_estimated_delivery_date as timestamp) end as estimated_delivery_at,

        -- metadata
        _ingested_at,
        _kafka_offset,
        _kafka_partition

    from source
    where order_id is not null
)

select * from cleaned