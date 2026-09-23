SELECT
    business_id,
    campaign_name,
    spend_usd,
    platform_conversions,
    initial_orders AS lightfunnels_orders,
    confirmed_orders,
    delivered_orders,
    returned_orders,
    cost_per_lightfunnels_order_usd AS cost_per_order_usd,
    cost_per_confirmed_order_usd AS cost_per_confirmed_usd,
    cost_per_delivered_order_usd AS cost_per_delivered_usd,
    confirmation_rate,
    delivery_rate,
    return_rate
FROM marts.sama_pilot_tiktok_campaign_outcomes
WHERE is_target_campaign
ORDER BY spend_usd DESC, campaign_name
