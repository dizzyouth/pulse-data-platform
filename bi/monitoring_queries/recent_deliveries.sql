SELECT attempted_at_utc,completed_at_utc,business_id,delivery_status,provider,destination_key,
       delivery_kind,attempt_number,escalation_level,severity,lifecycle_status,
       dataset_name,layer,title,error_message,delivery_id,alert_event_id
FROM monitoring_views.recent_deliveries
WHERE 1=1
[[AND business_id = {{business}}]]
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
[[AND severity = {{severity}}]]
[[AND lifecycle_status = {{lifecycle_status}}]]
[[AND delivery_status = {{delivery_status}}]]
[[AND provider = {{provider}}]]
[[AND attempted_date_utc >= CAST({{start_date}} AS date)]]
[[AND attempted_date_utc <= CAST({{end_date}} AS date)]]
ORDER BY attempted_at_utc DESC,delivery_id DESC
LIMIT 100
