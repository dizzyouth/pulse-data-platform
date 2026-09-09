SELECT
    business_id,
    event_date,
    currency,
    gross_revenue,
    completed_orders,
    units_sold,
    avg_order_value
FROM marts.revenue_by_day
ORDER BY business_id, event_date, currency;
