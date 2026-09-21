WITH excluded_cod AS (
    SELECT
        business_id,
        MAX(issue_count) AS excluded_records
    FROM marts.sama_pilot_data_quality
    WHERE check_name = 'eligible_cod_records_outside_cohort'
    GROUP BY business_id
),
summary AS (
    SELECT
        funnel.business_id,
        1 AS display_order,
        'Matched' AS identity_status,
        funnel.matched_orders AS record_count,
        'MATCHED_HIGH_CONFIDENCE' AS disposition
    FROM marts.sama_pilot_funnel AS funnel

    UNION ALL

    SELECT business_id, 2, 'Ambiguous', ambiguous_orders, 'REVIEW_REQUIRED'
    FROM marts.sama_pilot_funnel

    UNION ALL

    SELECT business_id, 3, 'Unmatched', unmatched_orders, 'UNRESOLVED'
    FROM marts.sama_pilot_funnel

    UNION ALL

    SELECT
        funnel.business_id,
        4,
        'Excluded COD outside cohort',
        COALESCE(excluded_cod.excluded_records, 0),
        'EXCLUDED'
    FROM marts.sama_pilot_funnel AS funnel
    LEFT JOIN excluded_cod USING (business_id)

    UNION ALL

    SELECT business_id, 5, 'Out of Stock', out_of_stock_orders, 'FINAL_ORDER_STATUS'
    FROM marts.sama_pilot_funnel
)
SELECT
    business_id,
    display_order,
    identity_status,
    record_count,
    disposition
FROM summary
ORDER BY business_id, display_order
