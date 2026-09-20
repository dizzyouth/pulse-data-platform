SELECT business_id, code, severity, classification, issue_count
FROM marts.olist_data_quality
ORDER BY CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END,
         issue_count DESC, code
