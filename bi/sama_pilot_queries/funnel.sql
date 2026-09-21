SELECT
    funnel.business_id,
    stage.stage_order,
    stage.stage_name,
    stage.order_count
FROM marts.sama_pilot_funnel AS funnel
CROSS JOIN LATERAL (
    VALUES
        (1, 'Initial Orders', funnel.initial_orders),
        (2, 'Matched Leads', funnel.matched_orders),
        (3, 'Confirmed', funnel.confirmed_leads),
        (4, 'Shipped', funnel.shipped_orders),
        (5, 'Delivered', funnel.delivered_orders),
        (6, 'Returned', funnel.returned_orders)
) AS stage(stage_order, stage_name, order_count)
ORDER BY funnel.business_id, stage.stage_order
