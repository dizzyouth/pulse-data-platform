SELECT business_id, report_date, platform, currency,
       SUM(platform_conversion_value) / NULLIF(SUM(spend), 0) AS platform_roas
FROM marts.marketing_overview
GROUP BY business_id, report_date, platform, currency
ORDER BY business_id, report_date, platform, currency;
