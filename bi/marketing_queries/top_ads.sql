SELECT business_id, report_date, platform, campaign_id, ad_group_id, ad_id, ad_name,
       currency, spend, clicks, platform_conversions, platform_roas, spend_rank
FROM marts.ad_performance
WHERE spend_rank <= 10
ORDER BY business_id, report_date DESC, platform, spend_rank;
