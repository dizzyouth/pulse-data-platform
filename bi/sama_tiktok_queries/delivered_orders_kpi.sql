SELECT
    business_id,
    SUM(delivered_orders)::bigint AS delivered_orders
FROM marts.sama_pilot_tiktok_campaign_outcomes
WHERE is_target_campaign
GROUP BY business_id
