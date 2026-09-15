SELECT business_id, cohort_date, currency,
       sum(variable_operational_cost - product_cogs) AS variable_operational_costs
FROM marts.unit_economics
GROUP BY business_id, cohort_date, currency
ORDER BY cohort_date, business_id, currency;
