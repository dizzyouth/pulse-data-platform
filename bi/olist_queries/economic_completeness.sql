SELECT business_id, currency, economic_status, order_count,
       cogs_available, attribution_available, profit_calculated
FROM marts.olist_economic_completeness
ORDER BY economic_status
