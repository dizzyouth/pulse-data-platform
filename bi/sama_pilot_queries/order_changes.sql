SELECT
    business_id,
    CONCAT(initial_quantity, ' → ', final_quantity) AS quantity_change,
    COALESCE(currency, 'Currency unavailable') AS currency,
    order_count,
    quantity_changed_orders,
    value_changed_orders,
    ROUND(initial_value::numeric, 2) AS initial_value,
    ROUND(final_value::numeric, 2) AS final_value
FROM marts.sama_pilot_order_changes
ORDER BY business_id, order_count DESC, initial_quantity, final_quantity, currency
