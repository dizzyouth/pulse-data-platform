SELECT business_id, order_created_date, currency, payment_type,
       count(*) AS unattributed_orders, sum(order_value) AS unattributed_order_value,
       sum(delivered_order_value) AS unattributed_delivered_order_value
FROM marts.commerce_economics
WHERE NOT has_marketing_attribution
GROUP BY business_id, order_created_date, currency, payment_type
ORDER BY order_created_date, business_id, currency, payment_type;
