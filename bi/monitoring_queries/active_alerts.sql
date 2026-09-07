SELECT lifecycle_status, severity, dataset_name, layer, title, occurrence_count,
       last_seen_at_utc, first_seen_at_utc, acknowledged_by, resolved_at_utc,
       resolved_by, resolution_note, duration_seconds, alert_event_id, incident_key
FROM monitoring_views.active_alerts
WHERE 1=1
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
[[AND severity = {{severity}}]]
[[AND lifecycle_status = {{lifecycle_status}}]]
ORDER BY last_seen_at_utc DESC, alert_event_id
LIMIT 100
