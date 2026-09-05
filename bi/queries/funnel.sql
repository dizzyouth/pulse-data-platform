SELECT
    event_date,
    country,
    product_views,
    cart_adds,
    checkouts_started,
    orders_created,
    payments_completed,
    view_to_cart_rate,
    cart_to_checkout_rate,
    checkout_to_order_rate,
    order_to_payment_rate
FROM marts.funnel_performance
ORDER BY event_date, country;
