SELECT business_id, cohort_date, currency, sum(marketing_spend) AS marketing_spend
FROM marts.unit_economics
GROUP BY business_id, cohort_date, currency
ORDER BY cohort_date, business_id, currency;
