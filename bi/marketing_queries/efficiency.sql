SELECT business_id, report_date, platform, currency,
       SUM(clicks)::double precision / NULLIF(SUM(impressions), 0) AS ctr,
       SUM(spend) / NULLIF(SUM(clicks), 0) AS cpc,
       SUM(spend) * 1000.0 / NULLIF(SUM(impressions), 0) AS cpm
FROM marts.marketing_overview
GROUP BY business_id, report_date, platform, currency
ORDER BY business_id, report_date, platform, currency;
