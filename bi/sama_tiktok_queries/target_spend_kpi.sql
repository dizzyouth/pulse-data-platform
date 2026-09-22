WITH target_spend AS (
    SELECT
        business_id,
        ROUND(SUM(spend_usd)::numeric, 2) AS target_campaign_spend_usd
    FROM marts.sama_pilot_tiktok_campaign_outcomes
    WHERE is_target_campaign
    GROUP BY business_id
)
SELECT
    business_id,
    target_campaign_spend_usd,
    CONCAT('$', TO_CHAR(target_campaign_spend_usd, 'FM999,999,990.00'))
        AS target_campaign_spend_display
FROM target_spend
