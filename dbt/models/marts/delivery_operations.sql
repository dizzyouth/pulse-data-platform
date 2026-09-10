SELECT business_id, cohort_date, courier, currency, payment_type,
       shipped_orders, delivered_orders, refused_orders, returned_orders,
       delivery_attempts, delivery_rate, refusal_rate, return_rate,
       average_delivery_attempts, average_time_to_ship_hours, average_time_to_deliver_hours
FROM {{ source('analytics', 'delivery_performance') }}
