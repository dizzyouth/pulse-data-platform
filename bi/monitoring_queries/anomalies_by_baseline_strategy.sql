SELECT baseline_strategy,sum(anomaly_count) AS anomaly_count
FROM monitoring_views.anomalies_by_strategy
WHERE 1=1
[[AND baseline_strategy = {{baseline_strategy}}]]
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
[[AND severity = {{severity}}]]
[[AND observed_date_utc >= {{start_date}}]]
[[AND observed_date_utc <= {{end_date}}]]
GROUP BY baseline_strategy
ORDER BY anomaly_count DESC,baseline_strategy
