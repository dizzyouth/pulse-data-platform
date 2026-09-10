SELECT business_id, cohort_date, provider, currency, payment_type, eligible_orders,
       confirmed_orders, confirmation_rate
FROM marts.confirmation_operations
ORDER BY cohort_date
