SELECT dataset_name, layer, overall_status, completed_at_utc, passed_checks,
       warning_checks, failed_checks, critical_failures, should_block, execution_source
FROM monitoring_views.latest_quality_status
WHERE 1=1
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
ORDER BY layer, dataset_name
