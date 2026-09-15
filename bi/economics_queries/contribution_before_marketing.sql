SELECT business_id, cohort_date, currency,
       sum(contribution_before_marketing) AS contribution_before_marketing
FROM marts.unit_economics
GROUP BY business_id, cohort_date, currency
ORDER BY cohort_date, business_id, currency;
