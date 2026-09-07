-- Current health and actual latest recorded execution per dataset/layer.
SELECT * FROM monitoring_views.current_health ORDER BY layer;
SELECT * FROM monitoring_views.latest_quality_status ORDER BY layer, dataset_name;

-- Caller-selected inclusive UTC date window. NULL layer means all layers.
-- Change EXECUTE arguments to inspect another period, without changing views.
PREPARE pulse_monitoring_window(date, date, text) AS
SELECT layer,
       count(*) FILTER (WHERE status = 'WARN') AS warning_checks,
       count(*) FILTER (WHERE status = 'FAIL' AND severity = 'CRITICAL') AS critical_failures
FROM monitoring_views.check_history
WHERE checked_date_utc BETWEEN $1 AND $2
  AND ($3 IS NULL OR layer = $3)
GROUP BY layer ORDER BY layer;
EXECUTE pulse_monitoring_window(CURRENT_DATE - 30, CURRENT_DATE, NULL);
DEALLOCATE pulse_monitoring_window;

SELECT * FROM monitoring_views.check_failure_summary
WHERE failure_count + warning_count > 0
ORDER BY failure_count + warning_count DESC, dataset_name, layer, check_name;

SELECT * FROM monitoring_views.recent_critical_failures
ORDER BY checked_at_utc DESC, quality_result_id DESC LIMIT 100;

SELECT * FROM monitoring_views.quality_history
WHERE dataset_name = 'daily_sales'
ORDER BY completed_at_utc DESC, quality_run_id DESC;
