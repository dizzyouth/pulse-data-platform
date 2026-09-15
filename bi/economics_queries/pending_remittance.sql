SELECT business_id, order_created_date, currency,
       sum(remittance_gap) AS pending_remittance
FROM marts.cod_economics
GROUP BY business_id, order_created_date, currency
ORDER BY order_created_date, business_id, currency;
