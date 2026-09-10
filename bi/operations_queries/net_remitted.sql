SELECT business_id, period_end, provider, currency, settlement_status,
       gross_collected, provider_fees, shipping_fees, cod_fees, adjustments, net_remitted
FROM marts.remittance_operations
ORDER BY period_end
