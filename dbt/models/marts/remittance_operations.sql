SELECT business_id, period_end, provider, currency, settlement_status,
       remittance_count, gross_collected, provider_fees, shipping_fees,
       cod_fees, adjustments, net_remitted, remittance_pending_amount
FROM {{ source('analytics', 'remittance_performance') }}
