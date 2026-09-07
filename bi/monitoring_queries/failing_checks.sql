SELECT dataset_name, layer, check_name,
       count(*) FILTER (WHERE status = 'WARN') AS warning_checks,
       count(*) FILTER (WHERE status = 'FAIL') AS failed_checks,
       count(*) FILTER (WHERE status = 'FAIL' AND severity = 'CRITICAL') AS critical_failures
FROM monitoring_views.check_history
WHERE status IN ('WARN', 'FAIL')
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
[[AND checked_date_utc >= {{start_date}}]]
[[AND checked_date_utc <= {{end_date}}]]
GROUP BY dataset_name, layer, check_name
ORDER BY count(*) DESC, dataset_name, layer, check_name
LIMIT 20
