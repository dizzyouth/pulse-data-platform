SELECT business_id, cohort_date, courier, currency, payment_type,
       delivered_orders, returned_orders, return_rate
FROM marts.delivery_operations
ORDER BY cohort_date
