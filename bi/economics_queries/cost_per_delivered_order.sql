SELECT business_id, cohort_date, currency, marketing_cost_per_delivered_order
FROM marts.unit_economics
ORDER BY cohort_date, business_id, currency;
