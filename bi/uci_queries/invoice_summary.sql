SELECT
    business_id,
    scoped_invoice_id,
    worksheet,
    raw_invoice_id,
    native_invoice_type,
    invoice_at,
    currency,
    country,
    anonymous_customer,
    line_count,
    net_ledger_value,
    commerce_projection_eligible
FROM marts.uci_invoice_summary
ORDER BY invoice_at DESC, scoped_invoice_id
