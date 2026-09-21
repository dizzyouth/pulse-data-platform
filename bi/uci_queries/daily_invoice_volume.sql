SELECT
    business_id,
    event_date,
    invoice_count,
    normal_invoice_count,
    cancellation_invoice_count
FROM marts.uci_retail_daily
ORDER BY event_date
