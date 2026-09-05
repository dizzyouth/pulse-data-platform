SELECT
    currency,
    SUM(gross_revenue) AS total_gross_revenue,
    SUM(completed_orders) AS completed_orders,
    SUM(units_sold) AS units_sold,
    SUM(gross_revenue) / NULLIF(SUM(completed_orders), 0) AS avg_order_value
FROM marts.revenue_by_day
GROUP BY currency
ORDER BY currency;
