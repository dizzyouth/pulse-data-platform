SELECT business_id, cohort_date, courier, currency, payment_type,
       shipped_orders, refused_orders, refusal_rate
FROM marts.delivery_operations
ORDER BY cohort_date
