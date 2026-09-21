SELECT
    business_id,
    currency,
    ROUND(SUM(collected_native_currency)::numeric, 2) AS collected_revenue,
    ROUND(SUM(cod_fee_native_currency)::numeric, 2) AS cod_fee
FROM marts.sama_pilot_native_economics
WHERE currency IS NOT NULL
GROUP BY business_id, currency
HAVING SUM(collected_native_currency) <> 0
    OR SUM(cod_fee_native_currency) <> 0
ORDER BY business_id, currency
