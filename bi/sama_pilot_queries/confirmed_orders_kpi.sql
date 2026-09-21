SELECT
    business_id,
    confirmed_leads AS confirmed_orders
FROM marts.sama_pilot_funnel
ORDER BY business_id
