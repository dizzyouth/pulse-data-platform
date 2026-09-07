SELECT metric_name,count(*) AS anomaly_count,
       count(*) FILTER (WHERE severity='CRITICAL') AS critical_anomalies
FROM monitoring_views.recent_anomalies
WHERE 1=1
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
[[AND severity = {{severity}}]]
[[AND (observed_at_utc AT TIME ZONE 'UTC')::date >= {{start_date}}]]
[[AND (observed_at_utc AT TIME ZONE 'UTC')::date <= {{end_date}}]]
GROUP BY metric_name
ORDER BY anomaly_count DESC,metric_name LIMIT 20
