CREATE SCHEMA IF NOT EXISTS monitoring_views;

-- Grain: one persisted dataset execution (including distinct retry attempts).
-- OFFSET 0 prevents automatic view updates without changing rows or ordering.
CREATE OR REPLACE VIEW monitoring_views.quality_history AS
SELECT r.*,
       (completed_at_utc AT TIME ZONE 'UTC')::date AS completed_date_utc,
       extract(epoch FROM completed_at_utc - started_at_utc) AS duration_seconds
FROM monitoring.quality_runs r
OFFSET 0;

-- Completion time defines latest, not logical date or worst historical status.
-- UUID breaks timestamp ties deterministically, without implying chronology.
CREATE OR REPLACE VIEW monitoring_views.latest_quality_status AS
SELECT DISTINCT ON (dataset_name, layer) *
FROM monitoring_views.quality_history
ORDER BY dataset_name, layer, completed_at_utc DESC, quality_run_id DESC;

-- Grain: one check result, with its execution context. Keep JSONB values typed.
CREATE OR REPLACE VIEW monitoring_views.check_history AS
SELECT q.*, r.dataset_name, r.layer, r.execution_source, r.execution_id,
       r.dag_id, r.airflow_run_id, r.task_id, r.attempt_number, r.map_index,
       r.logical_date_utc, r.overall_status AS run_status,
       (q.checked_at_utc AT TIME ZONE 'UTC')::date AS checked_date_utc
FROM monitoring.quality_results q
JOIN monitoring.quality_runs r USING (quality_run_id)
OFFSET 0;

-- Grain: dataset/layer/check across all history. Windows belong in consuming
-- queries over check_history, before aggregation, not over this summary.
CREATE OR REPLACE VIEW monitoring_views.check_failure_summary AS
SELECT dataset_name, layer, check_name,
       count(*) FILTER (WHERE status = 'FAIL') AS failure_count,
       count(*) FILTER (WHERE status = 'WARN') AS warning_count,
       count(*) FILTER (WHERE status = 'FAIL' AND severity = 'CRITICAL') AS critical_failure_count,
       max(checked_at_utc) FILTER (WHERE status = 'FAIL') AS latest_failure_at_utc,
       max(checked_at_utc) FILTER (WHERE status = 'WARN') AS latest_warning_at_utc
FROM monitoring_views.check_history
GROUP BY dataset_name, layer, check_name;

-- No embedded window or LIMIT: callers choose what "recent" means.
CREATE OR REPLACE VIEW monitoring_views.recent_critical_failures AS
SELECT * FROM monitoring_views.check_history
WHERE status = 'FAIL' AND severity = 'CRITICAL';

-- Grain: one required pipeline layer. UNKNOWN means missing dataset coverage.
-- A known blocking failure takes precedence over missing coverage.
CREATE OR REPLACE VIEW monitoring_views.current_health AS
WITH expected(dataset_name, layer) AS (VALUES
    ('silver_valid', 'silver'),
    ('daily_sales', 'gold'), ('customer_metrics', 'gold'),
    ('product_metrics', 'gold'), ('funnel_metrics', 'gold'),
    ('daily_sales', 'analytics'), ('customer_metrics', 'analytics'),
    ('product_metrics', 'analytics'), ('funnel_metrics', 'analytics')
), successful AS (
    SELECT layer, max(checked_at_utc) AS latest_successful_check_at_utc
    FROM monitoring_views.check_history JOIN expected USING (dataset_name, layer)
    WHERE status = 'PASS'
    GROUP BY layer
)
SELECT e.layer, count(*) AS expected_datasets, count(l.quality_run_id) AS observed_datasets,
       CASE WHEN bool_or(l.should_block) THEN 'FAIL'
            WHEN count(l.quality_run_id) < count(*) THEN 'UNKNOWN'
            WHEN bool_or(l.overall_status = 'WARN') THEN 'WARN'
            ELSE 'PASS' END AS overall_status,
       min(l.completed_at_utc) AS oldest_latest_completion_at_utc,
       max(l.completed_at_utc) AS latest_completion_at_utc,
       s.latest_successful_check_at_utc,
       coalesce(sum(l.warning_checks), 0) AS warning_checks,
       coalesce(sum(l.failed_checks), 0) AS failed_checks,
       coalesce(sum(l.critical_failures), 0) AS critical_failures,
       coalesce(bool_or(l.should_block), false) AS should_block
FROM expected e
LEFT JOIN monitoring_views.latest_quality_status l USING (dataset_name, layer)
LEFT JOIN successful s ON s.layer = e.layer
GROUP BY e.layer, s.latest_successful_check_at_utc;

-- No embedded time window: "recent" is chosen by the caller.
CREATE OR REPLACE VIEW monitoring_views.recent_anomalies AS
SELECT anomaly_id,evaluation_id,metric_name,dataset_name,layer,dimensions,current_value,
       baseline_value,deviation_value,deviation_percent,threshold,method,status,severity,
       observed_at_utc,evaluated_at_utc,history_count,explanation,execution_source,
       execution_id,dag_id,airflow_run_id,task_id,attempt_number,map_index,logical_date_utc,details
FROM monitoring.anomaly_results
WHERE status = 'ANOMALY'
OFFSET 0;

CREATE OR REPLACE VIEW monitoring_views.recent_alert_events AS
SELECT * FROM monitoring.alert_events
OFFSET 0;

-- Grain: one metric/dataset/layer/dimension series across persisted evaluations.
CREATE OR REPLACE VIEW monitoring_views.anomaly_summary_by_metric AS
SELECT metric_name,dataset_name,layer,dimensions,count(*) AS evaluations,
       count(*) FILTER (WHERE status='NORMAL') AS normal_count,
       count(*) FILTER (WHERE status='INSUFFICIENT_HISTORY') AS insufficient_history_count,
       count(*) FILTER (WHERE status='ANOMALY') AS anomaly_count,
       count(*) FILTER (WHERE status='ANOMALY' AND severity='CRITICAL') AS critical_anomaly_count,
       max(observed_at_utc) FILTER (WHERE status='ANOMALY') AS latest_anomaly_at_utc
FROM monitoring.anomaly_results
GROUP BY metric_name,dataset_name,layer,dimensions;

-- Grain: alert source/severity/status. Severity says urgency and status says lifecycle.
CREATE OR REPLACE VIEW monitoring_views.alert_summary_by_severity AS
SELECT source_type,severity,status,count(*) AS alert_count,max(created_at_utc) AS latest_alert_at_utc
FROM monitoring.alert_events
GROUP BY source_type,severity,status;
