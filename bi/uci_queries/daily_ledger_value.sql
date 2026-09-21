SELECT
    business_id,
    event_date,
    currency,
    positive_merchandise_value,
    cancellation_value,
    adjustment_value,
    non_merchandise_value,
    net_ledger_value
FROM marts.uci_retail_daily
ORDER BY event_date
