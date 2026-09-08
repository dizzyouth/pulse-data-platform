SELECT fallback_used,baseline_strategy,sum(evaluation_count) AS evaluation_count
FROM monitoring_views.baseline_fallback_summary
WHERE 1=1
[[AND baseline_strategy = {{baseline_strategy}}]]
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
[[AND severity = {{severity}}]]
[[AND observed_date_utc >= {{start_date}}]]
[[AND observed_date_utc <= {{end_date}}]]
GROUP BY fallback_used,baseline_strategy
ORDER BY fallback_used DESC,baseline_strategy
