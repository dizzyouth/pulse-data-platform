SELECT business_id, period_end, provider, currency, settlement_status,
       remittance_pending_amount
FROM marts.remittance_operations
ORDER BY period_end
