SELECT business_id, report_date, platform, currency,
       SUM(platform_conversions) AS platform_conversions,
       SUM(spend) / NULLIF(SUM(platform_conversions), 0) AS cpa
FROM marts.marketing_overview
GROUP BY business_id, report_date, platform, currency
ORDER BY business_id, report_date, platform, currency;
