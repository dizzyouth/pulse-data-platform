SELECT business_id, event_date, reporting_timezone, currency, payment_type,
       orders_created, confirmed_orders, shipped_orders, delivered_orders,
       refused_orders, returned_orders, unreachable_orders, delivery_attempts,
       cash_collected
FROM {{ source('analytics', 'order_operations_daily') }}
