SELECT checked_at_utc, dataset_name, layer, check_name, severity, status,
       observed_value, expected_value, dag_id, airflow_run_id, task_id, attempt_number
FROM monitoring_views.check_history
WHERE status = 'WARN'
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
[[AND checked_date_utc >= {{start_date}}]]
[[AND checked_date_utc <= {{end_date}}]]
ORDER BY checked_at_utc DESC, quality_result_id DESC
LIMIT 100
