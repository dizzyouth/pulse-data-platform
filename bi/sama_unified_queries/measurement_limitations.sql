SELECT
    business_id,
    CASE check_name
        WHEN 'platform_conversions_minus_lightfunnels_orders' THEN 'Platform conversions vs observed orders gap'
        WHEN 'ambiguous_identity_matches' THEN 'Ambiguous identities'
        WHEN 'unmatched_identity_records' THEN 'Unmatched identities'
        WHEN 'campaign_day_boundary_observations' THEN 'Campaign/day boundary observation'
        WHEN 'tiktok_campaigns_outside_target_cohort' THEN 'TikTok campaigns outside target cohort'
        WHEN 'zero_spend_rows_with_attributed_activity' THEN 'Zero-spend attributed event row'
    END AS measurement_limitation,
    issue_count
FROM marts.sama_pilot_tiktok_data_quality
WHERE check_name IN (
    'platform_conversions_minus_lightfunnels_orders',
    'ambiguous_identity_matches',
    'unmatched_identity_records',
    'campaign_day_boundary_observations',
    'tiktok_campaigns_outside_target_cohort',
    'zero_spend_rows_with_attributed_activity'
)
ORDER BY CASE check_name
    WHEN 'platform_conversions_minus_lightfunnels_orders' THEN 1
    WHEN 'ambiguous_identity_matches' THEN 2
    WHEN 'unmatched_identity_records' THEN 3
    WHEN 'campaign_day_boundary_observations' THEN 4
    WHEN 'tiktok_campaigns_outside_target_cohort' THEN 5
    WHEN 'zero_spend_rows_with_attributed_activity' THEN 6
END
