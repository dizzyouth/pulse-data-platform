SELECT business_id, event_date, currency,
       sum(order_value) AS order_value,
       sum(delivered_order_value) AS delivered_order_value,
       sum(cash_collected) AS cod_cash_collected,
       sum(net_remitted) AS net_remitted
FROM marts.economics_daily
GROUP BY business_id, event_date, currency
ORDER BY event_date, business_id, currency;
