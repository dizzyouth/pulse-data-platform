SELECT
    business_id,
    classification,
    severity,
    code,
    issue_count
FROM marts.uci_data_quality
ORDER BY issue_count DESC, code
