SELECT
    business_id,
    campaign_name,
    ROUND(spend_usd::numeric, 2) AS spend_usd,
    platform_conversions::bigint AS platform_conversions,
    initial_orders,
    confirmed_orders,
    delivered_orders,
    returned_orders,
    ROUND(cost_per_lightfunnels_order_usd::numeric, 2) AS cost_per_order_usd,
    ROUND(cost_per_confirmed_order_usd::numeric, 2) AS cost_per_confirmed_usd,
    ROUND(cost_per_delivered_order_usd::numeric, 2) AS cost_per_delivered_usd,
    delivery_rate,
    return_rate
FROM marts.sama_pilot_tiktok_campaign_outcomes
WHERE is_target_campaign
ORDER BY spend_usd DESC, campaign_name
