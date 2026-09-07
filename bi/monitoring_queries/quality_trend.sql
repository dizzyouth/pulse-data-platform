SELECT completed_date_utc, overall_status, count(*) AS quality_runs
FROM monitoring_views.quality_history
WHERE 1=1
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
[[AND overall_status = {{status}}]]
[[AND completed_date_utc >= {{start_date}}]]
[[AND completed_date_utc <= {{end_date}}]]
GROUP BY completed_date_utc, overall_status
ORDER BY completed_date_utc, overall_status
