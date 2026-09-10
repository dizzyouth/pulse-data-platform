SELECT business_id, currency, payment_type, current_operational_status,
       order_count, oldest_last_event_at
FROM marts.operations_overview
WHERE current_operational_status NOT IN ('DELIVERED','RETURNED_TO_ORIGIN','REMITTED','CANCELLED','REJECTED_CONFIRMATION')
  AND oldest_last_event_at < current_timestamp - interval '48 hours'
ORDER BY oldest_last_event_at
