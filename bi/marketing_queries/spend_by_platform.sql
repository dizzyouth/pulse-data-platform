SELECT business_id, platform, currency, SUM(spend) AS spend
FROM marts.marketing_overview
GROUP BY business_id, platform, currency
ORDER BY business_id, currency, spend DESC;
