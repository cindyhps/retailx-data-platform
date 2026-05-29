{{ config(materialized='table') }}

select
    order_id,
    count(order_item_id)    as total_items,
    sum(price)              as total_price,
    sum(freight_value)      as total_freight,
    sum(total_item_value)   as total_order_value
from {{ ref('stg_order_items') }}
group by order_id