SELECT
    business_id,
    COALESCE(currency, 'Currency unavailable') AS currency,
    economic_status,
    CASE
        WHEN economic_status = 'FX_REQUIRED'
            THEN 'Cross-currency contribution unavailable — trusted FX required'
        WHEN economic_status = 'SEPARATE_CURRENCIES'
            THEN 'Native revenue and USD costs remain separate — no contribution calculated'
        ELSE 'Contribution remains unavailable until the economics are complete'
    END AS economic_limitation
FROM marts.sama_pilot_native_economics
GROUP BY business_id, currency, economic_status
ORDER BY business_id, currency, economic_status
