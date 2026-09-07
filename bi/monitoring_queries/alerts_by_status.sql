SELECT lifecycle_status, count(*) AS alert_count
FROM monitoring_views.alert_history
WHERE 1=1
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
[[AND severity = {{severity}}]]
[[AND lifecycle_status = {{lifecycle_status}}]]
[[AND last_seen_date_utc >= CAST({{start_date}} AS date)]]
[[AND last_seen_date_utc <= CAST({{end_date}} AS date)]]
GROUP BY lifecycle_status
ORDER BY lifecycle_status
