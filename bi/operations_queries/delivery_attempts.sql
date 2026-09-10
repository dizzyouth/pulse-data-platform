SELECT business_id, cohort_date, courier, currency, payment_type,
       delivery_attempts, average_delivery_attempts
FROM marts.delivery_operations
ORDER BY cohort_date
