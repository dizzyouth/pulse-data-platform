SELECT business_id, order_created_date, currency, payment_type, economic_status,
       count(*) AS orders, sum(missing_cogs_lines) AS missing_cogs_lines
FROM marts.commerce_economics
WHERE economic_status <> 'COMPLETE'
GROUP BY business_id, order_created_date, currency, payment_type, economic_status
ORDER BY order_created_date, business_id, currency, payment_type, economic_status;
