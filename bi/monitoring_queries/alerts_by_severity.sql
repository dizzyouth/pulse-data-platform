SELECT severity,count(*) AS alert_count,
       count(*) FILTER (WHERE source_type='QUALITY_FAILURE') AS quality_failure_alerts,
       count(*) FILTER (WHERE source_type='ANOMALY') AS anomaly_alerts
FROM monitoring_views.recent_alert_events
WHERE 1=1
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
[[AND severity = {{severity}}]]
[[AND (created_at_utc AT TIME ZONE 'UTC')::date >= {{start_date}}]]
[[AND (created_at_utc AT TIME ZONE 'UTC')::date <= {{end_date}}]]
GROUP BY severity
ORDER BY severity
