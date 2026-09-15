SELECT business_id, cohort_date, currency, contribution_margin_pct
FROM marts.unit_economics
ORDER BY cohort_date, business_id, currency;
