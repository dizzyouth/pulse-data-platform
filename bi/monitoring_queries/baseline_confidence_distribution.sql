SELECT confidence,sum(evaluation_count) AS evaluation_count
FROM monitoring_views.anomaly_confidence_summary
WHERE 1=1
[[AND confidence = {{confidence}}]]
[[AND baseline_strategy = {{baseline_strategy}}]]
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
[[AND severity = {{severity}}]]
[[AND observed_date_utc >= {{start_date}}]]
[[AND observed_date_utc <= {{end_date}}]]
GROUP BY confidence
ORDER BY CASE confidence WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END
