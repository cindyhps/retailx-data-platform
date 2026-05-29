{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'order_item_id']
    )
}}
with source as (
    select * from {{ source('raw', 'order_items') }}
),
cleaned as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        cast(price as decimal(10,2))                 as price,
        cast(freight_value as decimal(10,2))          as freight_value,
        cast(price + freight_value as decimal(10,2))  as total_item_value,
        _ingested_at,
        _kafka_offset,
        _kafka_partition
    from source
    where order_id is not null
)
select * from cleaned
