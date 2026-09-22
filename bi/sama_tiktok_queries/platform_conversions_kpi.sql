SELECT
    business_id,
    SUM(platform_conversions)::bigint AS platform_conversions
FROM marts.sama_pilot_tiktok_campaign_outcomes
WHERE is_target_campaign
GROUP BY business_id
