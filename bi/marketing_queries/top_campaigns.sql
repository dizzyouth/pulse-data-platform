SELECT business_id, report_date, platform, campaign_id, campaign_name, currency,
       spend, platform_conversions, platform_roas, spend_rank
FROM marts.campaign_performance
WHERE spend_rank <= 10
ORDER BY business_id, report_date DESC, platform, spend_rank;
