SELECT escalated_at_utc,business_id,escalation_level,provider,destination_key,lifecycle_status,
       severity,dataset_name,layer,title,occurrence_count,first_seen_at_utc,
       last_seen_at_utc,alert_event_id
FROM monitoring_views.escalation_summary
WHERE 1=1
[[AND business_id = {{business}}]]
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
[[AND severity = {{severity}}]]
[[AND lifecycle_status = {{lifecycle_status}}]]
[[AND provider = {{provider}}]]
[[AND (escalated_at_utc AT TIME ZONE 'UTC')::date >= CAST({{start_date}} AS date)]]
[[AND (escalated_at_utc AT TIME ZONE 'UTC')::date <= CAST({{end_date}} AS date)]]
ORDER BY escalated_at_utc DESC,alert_event_id
LIMIT 100
