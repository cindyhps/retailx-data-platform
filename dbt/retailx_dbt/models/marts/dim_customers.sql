{{
    config(
        materialized='table'
    )
}}

-- Purpose: 
-- > Create a dimension table for customers
-- > Enriched with order statistics such as total orders, first and last order dates, and total spent.

with customers as (
    select distinct
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix    as zip_code,
        customer_city               as city,
        customer_state              as state
    from {{ source('raw', 'customers') }}
),

order_stats as (
    select
        customer_id,
        count(order_id)             as total_orders,
        min(ordered_at)             as first_order_at,
        max(ordered_at)             as last_order_at,
        sum(total_payment_value)    as total_spent
    from {{ ref('fct_orders') }}
    group by customer_id
)

select
    c.customer_id,
    c.customer_unique_id,
    c.zip_code,
    c.city,
    c.state,
    coalesce(s.total_orders, 0)     as total_orders,
    s.first_order_at,
    s.last_order_at,
    coalesce(s.total_spent, 0)      as total_spent
from customers c
left join order_stats s on c.customer_id = s.customer_id