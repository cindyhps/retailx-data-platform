-- Purpose: 
-- > Aggregate daily revenue metrics including total orders
-- > Delivered orders, cancelled orders, total revenue, average order value, average delivery days, and delivery rate percentage.

with daily as (
    select
        order_date,
        order_month,
        count(order_id)                 as total_orders,
        count(case when order_status = 'delivered'
            then 1 end)                 as delivered_orders,
        count(case when order_status = 'cancelled'
            then 1 end)                 as cancelled_orders,
        sum(total_payment_value)        as total_revenue,
        avg(total_payment_value)        as avg_order_value,
        avg(delivery_days)              as avg_delivery_days
    from {{ ref('fct_orders') }}
    where order_date is not null
        and try_cast(order_date as timestamp) is not null -- filter out invalid dates
    group by order_date, order_month
)


select
    order_date,
    order_month,
    total_orders,
    delivered_orders,
    cancelled_orders,
    round(total_revenue, 2)         as total_revenue,
    round(avg_order_value, 2)       as avg_order_value,
    round(avg_delivery_days, 1)     as avg_delivery_days,
    round(delivered_orders * 100.0
        / nullif(total_orders, 0), 1) as delivery_rate_pct
from daily
order by order_date