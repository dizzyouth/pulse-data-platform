SELECT business_id, cohort_date, courier, currency, payment_type,
       shipped_orders, delivered_orders, delivery_rate
FROM marts.delivery_operations
ORDER BY cohort_date
