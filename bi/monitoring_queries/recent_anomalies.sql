SELECT observed_at_utc,dataset_name,layer,metric_name,dimensions,severity,current_value,
       baseline_value,deviation_value,deviation_percent,method,history_count,explanation
FROM monitoring_views.recent_anomalies
WHERE 1=1
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
[[AND severity = {{severity}}]]
[[AND (observed_at_utc AT TIME ZONE 'UTC')::date >= {{start_date}}]]
[[AND (observed_at_utc AT TIME ZONE 'UTC')::date <= {{end_date}}]]
ORDER BY observed_at_utc DESC,anomaly_id DESC LIMIT 100
