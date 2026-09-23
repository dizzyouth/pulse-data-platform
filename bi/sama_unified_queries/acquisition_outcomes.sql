SELECT business_id, 1 AS stage_order, 'Platform Conversions' AS stage_name,
       platform_conversions AS stage_count
FROM marts.sama_pilot_unified_overview
UNION ALL
SELECT business_id, 2, 'Lightfunnels Orders', lightfunnels_orders
FROM marts.sama_pilot_unified_overview
UNION ALL
SELECT business_id, 3, 'Confirmed Orders', confirmed_orders
FROM marts.sama_pilot_unified_overview
UNION ALL
SELECT business_id, 4, 'Shipped Orders', shipped_orders
FROM marts.sama_pilot_unified_overview
UNION ALL
SELECT business_id, 5, 'Delivered Orders (terminal outcome)', delivered_orders
FROM marts.sama_pilot_unified_overview
UNION ALL
SELECT business_id, 6, 'Returned Orders (terminal outcome)', returned_orders
FROM marts.sama_pilot_unified_overview
