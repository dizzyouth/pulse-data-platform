SELECT provider,delivery_status,sum(delivery_attempts) AS delivery_attempts
FROM monitoring_views.delivery_summary_by_provider
WHERE 1=1
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
[[AND severity = {{severity}}]]
[[AND lifecycle_status = {{lifecycle_status}}]]
[[AND delivery_status = {{delivery_status}}]]
[[AND provider = {{provider}}]]
[[AND attempted_date_utc >= CAST({{start_date}} AS date)]]
[[AND attempted_date_utc <= CAST({{end_date}} AS date)]]
GROUP BY provider,delivery_status
ORDER BY provider,delivery_status
