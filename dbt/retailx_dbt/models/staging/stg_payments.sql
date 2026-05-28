-- Purpose: Clean and transform raw payments data, ensuring correct data types and filtering out invalid records

with source as (
    select * from {{ source('raw', 'payments') }}
),

cleaned as (
    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        cast(payment_value as decimal(10,2)) as payment_value,

        -- metadata
        _ingested_at,
        _kafka_offset,
        _kafka_partition

    from source
    where order_id is not null
        and payment_value >= 0
)

select * from cleaned