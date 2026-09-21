SELECT
    business_id,
    event_date,
    invoice_count,
    anonymous_customer_invoice_count,
    anonymous_customer_rate
FROM marts.uci_retail_daily
ORDER BY event_date
