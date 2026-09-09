SELECT business_id, report_date, platform, currency, SUM(spend) AS spend
FROM marts.marketing_overview
GROUP BY business_id, report_date, platform, currency
ORDER BY business_id, report_date, platform, currency;
