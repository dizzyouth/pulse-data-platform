SELECT business_id, native_status, order_count
FROM marts.olist_orders_by_status
ORDER BY order_count DESC, native_status
