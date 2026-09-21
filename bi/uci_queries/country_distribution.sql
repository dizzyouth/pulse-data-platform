SELECT
    business_id,
    country,
    currency,
    invoice_count,
    line_count,
    net_ledger_value
FROM marts.uci_country_distribution
ORDER BY invoice_count DESC, country
