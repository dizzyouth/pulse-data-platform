select
    business_id,
    currency,
    economic_status,
    invoice_count,
    cogs_available,
    merchant_shipping_cost_available,
    attribution_available,
    cod_available,
    remittance_available,
    profit_calculated
from {{ source('analytics', 'uci_economic_completeness') }}
