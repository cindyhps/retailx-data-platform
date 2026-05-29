{{ config(materialized='table') }}

select
    order_id,
    sum(payment_value)          as total_payment_value,
    count(payment_sequential)   as total_payment_installments,
    max(payment_type)           as payment_type
from {{ ref('stg_payments') }}
group by order_id