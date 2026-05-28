-- Purpose: Clean and transform raw order items data, ensuring correct data types and filtering out invalid records

with source as (
    select * from {{ source('raw', 'order_items') }}
),

cleaned as (
    select
        order_id,
        cast(order_item_id as integer) as order_item_id,
        product_id,
        seller_id,
        cast(shipping_limit_date as timestamp) as shipping_limit_at,
        cast(price as decimal(10,2))           as price,
        cast(freight_value as decimal(10,2))   as freight_value,
        price + freight_value                  as total_item_value,

        -- metadata
        _ingested_at,
        _kafka_offset,
        _kafka_partition

    from source
    where order_id is not null
        and price >= 0
        and freight_value >= 0
)

select * from cleaned