SELECT
    business_id,
    ROUND(
        SUM(spend_usd)::numeric / NULLIF(SUM(delivered_orders), 0),
        2
    ) AS cost_per_delivered_order_usd
FROM marts.sama_pilot_tiktok_campaign_outcomes
WHERE is_target_campaign
GROUP BY business_id
