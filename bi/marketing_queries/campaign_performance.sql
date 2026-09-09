SELECT business_id, report_date, platform, campaign_id, campaign_name, currency,
       spend, impressions, clicks, platform_conversions, ctr, cpc, cpm, cpa, platform_roas
FROM marts.campaign_performance
ORDER BY business_id, report_date DESC, spend DESC;
