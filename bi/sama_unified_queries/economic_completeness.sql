SELECT
    business_id,
    'FX_REQUIRED' AS economic_status,
    'Marketing and known operating costs are USD. COD collections are retained in native currencies. Cross-currency profit, contribution, margin and business ROAS are unavailable until a trusted FX source is supplied.' AS economic_limitation
FROM marts.sama_pilot_unified_overview
