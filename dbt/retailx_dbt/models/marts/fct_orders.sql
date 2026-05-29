{{ config(materialized='table') }}

select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.ordered_at,
    o.approved_at,
    o.delivered_carrier_at,
    o.delivered_customer_at,
    o.estimated_delivery_at,

    coalesce(i.total_items, 0)                  as total_items,
    coalesce(i.total_price, 0)                  as total_price,
    coalesce(i.total_freight, 0)                as total_freight,
    coalesce(i.total_order_value, 0)            as total_order_value,

    coalesce(p.total_payment_value, 0)          as total_payment_value,
    coalesce(p.total_payment_installments, 0)   as total_payment_installments,
    p.payment_type,

    date_trunc('day', o.ordered_at)             as order_date,
    date_trunc('month', o.ordered_at)           as order_month,

    case
        when o.delivered_customer_at is not null
        then datediff('day', o.ordered_at, o.delivered_customer_at)
        else null
    end as delivery_days

from {{ ref('stg_orders') }} o
left join {{ ref('int_order_items_agg') }} i on o.order_id = i.order_id
left join {{ ref('int_payments_agg') }} p on o.order_id = p.order_id
where o.ordered_at is not null