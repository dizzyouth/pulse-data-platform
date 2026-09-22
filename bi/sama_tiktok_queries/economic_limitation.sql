SELECT DISTINCT
    business_id,
    'USD' AS marketing_spend_currency,
    'NATIVE_CURRENCIES' AS cod_revenue_currency,
    'FX_REQUIRED' AS economic_status,
    'Marketing spend is USD while COD revenue is native currency. Cross-currency ROAS/contribution is unavailable until trusted FX is supplied.'
        AS economic_limitation
FROM marts.sama_pilot_tiktok_campaign_outcomes
