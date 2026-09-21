select
    business_id,
    country,
    currency,
    line_count,
    invoice_count,
    net_ledger_value
from {{ source('analytics', 'uci_country_distribution') }}
