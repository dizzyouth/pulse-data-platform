SELECT
    business_id,
    report_date,
    lightfunnels_orders,
    confirmed_orders,
    delivered_orders,
    returned_orders
FROM marts.sama_pilot_unified_daily
ORDER BY report_date
