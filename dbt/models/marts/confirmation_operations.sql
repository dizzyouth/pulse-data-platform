SELECT business_id, cohort_date, provider, currency, payment_type,
       eligible_orders, confirmed_orders, rejected_orders, unreachable_orders,
       confirmation_rate, unreachable_rate, average_time_to_confirm_hours
FROM {{ source('analytics', 'confirmation_performance') }}
