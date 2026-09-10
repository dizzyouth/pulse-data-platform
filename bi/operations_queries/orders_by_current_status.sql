SELECT business_id, currency, payment_type, current_operational_status, order_count
FROM marts.operations_overview
ORDER BY order_count DESC
