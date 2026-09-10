SELECT business_id, currency, payment_type, current_operational_status,
       count(*) AS order_count,
       sum(order_value) AS order_value,
       sum(cash_collected) AS cod_cash_collected,
       min(last_event_at) AS oldest_last_event_at
FROM {{ source('analytics', 'order_operations_current') }}
GROUP BY business_id, currency, payment_type, current_operational_status
