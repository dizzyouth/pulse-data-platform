SELECT business_id, cohort_date, courier, currency, payment_type, shipped_orders,
       delivered_orders, refused_orders, returned_orders, delivery_rate,
       refusal_rate, return_rate, average_delivery_attempts
FROM marts.delivery_operations
ORDER BY cohort_date, courier
