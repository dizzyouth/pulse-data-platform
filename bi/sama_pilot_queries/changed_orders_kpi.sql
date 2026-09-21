SELECT
    business_id,
    SUM(
        CASE
            WHEN quantity_changed_orders > 0 THEN order_count
            ELSE value_changed_orders
        END
    )::bigint AS changed_orders
FROM marts.sama_pilot_order_changes
GROUP BY business_id
ORDER BY business_id
