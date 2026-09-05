SELECT
    country,
    SUM(product_views) AS product_views,
    SUM(cart_adds) AS cart_adds,
    SUM(checkouts_started) AS checkouts_started,
    SUM(orders_created) AS orders_created,
    SUM(payments_completed) AS payments_completed,
    SUM(cart_adds)::DOUBLE PRECISION / NULLIF(SUM(product_views), 0) AS view_to_cart_rate,
    SUM(payments_completed)::DOUBLE PRECISION / NULLIF(SUM(orders_created), 0) AS order_to_payment_rate
FROM marts.funnel_performance
GROUP BY country
ORDER BY payments_completed DESC, country;
