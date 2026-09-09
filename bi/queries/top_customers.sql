SELECT
    business_id,
    customer_id,
    total_units_purchased,
    payments_completed,
    distinct_orders,
    DENSE_RANK() OVER (PARTITION BY business_id ORDER BY total_units_purchased DESC) AS purchase_rank
FROM marts.top_customers
ORDER BY business_id, purchase_rank, customer_id
LIMIT 20;
