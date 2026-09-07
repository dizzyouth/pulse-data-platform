SELECT created_at_utc,source_type,dataset_name,layer,severity,status,title,message,
       dag_id,airflow_run_id,task_id,attempt_number
FROM monitoring_views.recent_alert_events
WHERE 1=1
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
[[AND severity = {{severity}}]]
[[AND (created_at_utc AT TIME ZONE 'UTC')::date >= {{start_date}}]]
[[AND (created_at_utc AT TIME ZONE 'UTC')::date <= {{end_date}}]]
ORDER BY created_at_utc DESC,alert_event_id DESC LIMIT 100
