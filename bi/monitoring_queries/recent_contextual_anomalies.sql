SELECT observed_at_utc,business_id,dataset_name,layer,metric_name,dimensions,severity,
       baseline_strategy,confidence,observed_value,expected_value,lower_bound,upper_bound,
       trend_slope,seasonal_reference_count,training_window_size,model_error,fallback_used,explanation
FROM monitoring_views.anomaly_baseline_history
WHERE status='ANOMALY'
[[AND business_id = {{business}}]]
[[AND confidence = {{confidence}}]]
[[AND baseline_strategy = {{baseline_strategy}}]]
[[AND layer = {{layer}}]]
[[AND dataset_name = {{dataset}}]]
[[AND severity = {{severity}}]]
[[AND (observed_at_utc AT TIME ZONE 'UTC')::date >= {{start_date}}]]
[[AND (observed_at_utc AT TIME ZONE 'UTC')::date <= {{end_date}}]]
ORDER BY observed_at_utc DESC,anomaly_id DESC LIMIT 100
