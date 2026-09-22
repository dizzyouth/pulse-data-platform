WITH totals AS (
    SELECT
        business_id,
        SUM(platform_conversions)::bigint AS platform_conversions,
        SUM(initial_orders)::bigint AS lightfunnels_orders,
        SUM(confirmed_orders)::bigint AS confirmed_orders,
        SUM(shipped_orders)::bigint AS shipped_orders,
        SUM(delivered_orders)::bigint AS delivered_orders,
        SUM(returned_orders)::bigint AS returned_orders
    FROM marts.sama_pilot_tiktok_campaign_outcomes
    WHERE is_target_campaign
    GROUP BY business_id
)
SELECT
    totals.business_id,
    stage.stage_order,
    stage.stage_name,
    stage.stage_count
FROM totals
CROSS JOIN LATERAL (
    VALUES
        (1, 'TikTok Platform Conversions', totals.platform_conversions),
        (2, 'Lightfunnels Orders', totals.lightfunnels_orders),
        (3, 'Confirmed Orders', totals.confirmed_orders),
        (4, 'Shipped Orders', totals.shipped_orders),
        (5, 'Delivered Orders (Terminal Outcome)', totals.delivered_orders),
        (5, 'Returned Orders (Terminal Outcome)', totals.returned_orders)
) AS stage(stage_order, stage_name, stage_count)
ORDER BY totals.business_id, stage.stage_order, stage.stage_name
