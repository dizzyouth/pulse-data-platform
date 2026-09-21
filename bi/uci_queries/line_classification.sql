SELECT
    business_id,
    line_classification,
    currency,
    line_count,
    signed_line_value
FROM marts.uci_line_classification
ORDER BY line_count DESC, line_classification
