SELECT
    business_id,
    check_name,
    category,
    issue_count,
    observed_value,
    expected_value,
    status
FROM marts.sama_pilot_tiktok_data_quality
WHERE issue_count > 0 OR status = 'FAIL'
ORDER BY
    CASE status WHEN 'FAIL' THEN 1 WHEN 'OBSERVED' THEN 2 ELSE 3 END,
    issue_count DESC,
    check_name
