WITH totals AS (
    SELECT
        business_id,
        SUM(product_cogs_usd) AS product_cogs_usd,
        SUM(call_center_cost_usd) AS call_center_cost_usd,
        SUM(logistics_cost_usd) AS logistics_cost_usd,
        SUM(known_operational_cost_usd) AS known_operational_cost_usd
    FROM marts.sama_pilot_native_economics
    GROUP BY business_id
)
SELECT
    totals.business_id,
    cost.cost_order,
    cost.cost_category,
    ROUND(cost.amount_usd::numeric, 2) AS amount_usd
FROM totals
CROSS JOIN LATERAL (
    VALUES
        (1, 'Product COGS', totals.product_cogs_usd),
        (2, 'Call Center', totals.call_center_cost_usd),
        (3, 'Logistics', totals.logistics_cost_usd),
        (4, 'Known Operational Cost', totals.known_operational_cost_usd)
) AS cost(cost_order, cost_category, amount_usd)
ORDER BY totals.business_id, cost.cost_order
