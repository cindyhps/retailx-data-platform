-- Purpose: Aggregate order items at order level and payments at order level, then join to orders

with orders as (
    select * from {{ ref('stg_orders') }}
    where ordered_at is not null
),

order_items as (
    select
        order_id,
        count(order_item_id)        as total_items,
        sum(price)                  as total_price,
        sum(freight_value)          as total_freight,
        sum(total_item_value)       as total_order_value
    from {{ ref('stg_order_items') }}
    group by order_id
),

payments as (
    select
        order_id,
        sum(payment_value)          as total_payment_value,
        count(payment_sequential)   as total_payment_installments,
        max(payment_type)           as payment_type
    from {{ ref('stg_payments') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.ordered_at,
    o.approved_at,
    o.delivered_carrier_at,
    o.delivered_customer_at,
    o.estimated_delivery_at,

    -- order items metrics
    coalesce(i.total_items, 0)          as total_items,
    coalesce(i.total_price, 0)          as total_price,
    coalesce(i.total_freight, 0)        as total_freight,
    coalesce(i.total_order_value, 0)    as total_order_value,

    -- payment metrics
    coalesce(p.total_payment_value, 0)      as total_payment_value,
    coalesce(p.total_payment_installments, 0) as total_payment_installments,
    p.payment_type,

    -- derived
    date_trunc('day', o.ordered_at)     as order_date,
    date_trunc('month', o.ordered_at)   as order_month,

    -- delivery time in days
    case
        when o.delivered_customer_at is not null
        then datediff('day', o.ordered_at, o.delivered_customer_at)
        else null
    end as delivery_days

from orders o
left join order_items i on o.order_id = i.order_id
left join payments p on o.order_id = p.order_id