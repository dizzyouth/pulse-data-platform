SELECT business_id, 1 AS cost_order, 'TikTok Marketing Spend' AS cost_category,
       marketing_spend_usd AS amount_usd
FROM marts.sama_pilot_unified_overview
UNION ALL
SELECT business_id, 2, 'Product COGS', product_cogs_usd
FROM marts.sama_pilot_unified_overview
UNION ALL
SELECT business_id, 3, 'Call Center', call_center_cost_usd
FROM marts.sama_pilot_unified_overview
UNION ALL
SELECT business_id, 4, 'Logistics', logistics_cost_usd
FROM marts.sama_pilot_unified_overview
