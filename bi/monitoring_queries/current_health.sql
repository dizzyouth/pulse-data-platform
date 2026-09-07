SELECT layer, overall_status, observed_datasets, expected_datasets,
       oldest_latest_completion_at_utc, latest_completion_at_utc,
       latest_successful_check_at_utc, warning_checks, critical_failures, should_block
FROM monitoring_views.current_health
WHERE 1=1
[[AND layer = {{layer}}]]
ORDER BY layer
