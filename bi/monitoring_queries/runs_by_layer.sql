SELECT layer, count(*) AS quality_runs,
       coalesce(sum(warning_checks), 0) AS warning_checks,
       coalesce(sum(critical_failures), 0) AS critical_failures
FROM monitoring_views.quality_history
WHERE 1=1
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
[[AND overall_status = {{status}}]]
[[AND completed_date_utc >= {{start_date}}]]
[[AND completed_date_utc <= {{end_date}}]]
GROUP BY layer
ORDER BY layer
