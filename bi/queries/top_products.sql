SELECT
    product_id,
    seller_id,
    units_sold,
    payments_completed,
    distinct_customers,
    DENSE_RANK() OVER (ORDER BY units_sold DESC) AS purchase_rank
FROM marts.top_products
ORDER BY purchase_rank, product_id
LIMIT 20;
