SELECT business_id, payment_method, currency, payment_rows, payment_amount
FROM marts.olist_payment_methods
ORDER BY payment_amount DESC, payment_method
