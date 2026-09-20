SELECT business_id, event_date, currency, orders, merchandise_value,
       customer_freight_charge, commerce_calculated_total, payment_total,
       aggregate_payment_reconciliation_delta
FROM marts.olist_commerce_daily
ORDER BY event_date
