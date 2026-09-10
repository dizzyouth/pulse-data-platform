SELECT business_id, cohort_date, provider, currency, payment_type,
       eligible_orders, unreachable_orders, unreachable_rate
FROM marts.confirmation_operations
ORDER BY cohort_date
