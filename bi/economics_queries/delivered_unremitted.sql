SELECT business_id, cohort_date, currency,
       sum(delivered_but_unremitted_count) AS delivered_but_unremitted_orders,
       sum(delivered_but_unremitted_value) AS delivered_but_unremitted_value
FROM marts.unit_economics
GROUP BY business_id, cohort_date, currency
ORDER BY cohort_date, business_id, currency;
