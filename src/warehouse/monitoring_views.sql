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
    ('marketing_silver', 'silver'),
    ('daily_sales', 'gold'), ('customer_metrics', 'gold'),
    ('product_metrics', 'gold'), ('funnel_metrics', 'gold'),
    ('daily_sales', 'analytics'), ('customer_metrics', 'analytics'),
    ('product_metrics', 'analytics'), ('funnel_metrics', 'analytics'),
    ('marketing_daily', 'gold'), ('campaign_performance', 'gold'),
    ('ad_group_performance', 'gold'), ('ad_performance', 'gold'),
    ('marketing_daily', 'analytics'), ('campaign_performance', 'analytics'),
    ('ad_group_performance', 'analytics'), ('ad_performance', 'analytics')
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
       execution_id,dag_id,airflow_run_id,task_id,attempt_number,map_index,logical_date_utc,details,
       dimensions->>'business_id' AS business_id
FROM monitoring.anomaly_results
WHERE status = 'ANOMALY'
OFFSET 0;

CREATE OR REPLACE VIEW monitoring_views.recent_alert_events AS
SELECT a.*,a.details->'dimensions'->>'business_id' AS business_id
FROM monitoring.alert_events a
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

-- Contextual fields remain JSONB in the write model for backward compatibility,
-- and are projected here as typed, read-only reporting columns.
CREATE OR REPLACE VIEW monitoring_views.anomaly_baseline_history AS
SELECT anomaly_id,evaluation_id,metric_name,dataset_name,layer,dimensions,severity,status,
       observed_at_utc,evaluated_at_utc,current_value AS observed_value,
       coalesce((details->>'expected_value')::double precision,baseline_value) AS expected_value,
       (details->>'lower_bound')::double precision AS lower_bound,
       (details->>'upper_bound')::double precision AS upper_bound,
       coalesce(details->>'baseline_strategy','robust_history') AS baseline_strategy,
       (details->>'trend_slope')::double precision AS trend_slope,
       coalesce((details->>'seasonal_reference_count')::integer,0) AS seasonal_reference_count,
       coalesce((details->>'training_window_size')::integer,history_count) AS training_window_size,
       (details->>'model_error')::double precision AS model_error,
       coalesce((details->>'fallback_used')::boolean,false) AS fallback_used,
       coalesce(details->>'confidence','LOW') AS confidence,explanation,
       dimensions->>'business_id' AS business_id
FROM monitoring.anomaly_results
OFFSET 0;

-- Daily grains retain compatible dashboard dimensions before presentation rollup.
CREATE OR REPLACE VIEW monitoring_views.anomalies_by_strategy AS
SELECT baseline_strategy,dataset_name,layer,severity,status,
       (observed_at_utc AT TIME ZONE 'UTC')::date AS observed_date_utc,
       count(*) AS evaluation_count,
       count(*) FILTER (WHERE status='ANOMALY') AS anomaly_count,business_id
FROM monitoring_views.anomaly_baseline_history
GROUP BY baseline_strategy,dataset_name,layer,severity,status,business_id,
         (observed_at_utc AT TIME ZONE 'UTC')::date;

CREATE OR REPLACE VIEW monitoring_views.anomaly_confidence_summary AS
SELECT confidence,baseline_strategy,dataset_name,layer,severity,status,
       (observed_at_utc AT TIME ZONE 'UTC')::date AS observed_date_utc,
       count(*) AS evaluation_count,
       count(*) FILTER (WHERE status='ANOMALY') AS anomaly_count,business_id
FROM monitoring_views.anomaly_baseline_history
GROUP BY confidence,baseline_strategy,dataset_name,layer,severity,status,business_id,
         (observed_at_utc AT TIME ZONE 'UTC')::date;

CREATE OR REPLACE VIEW monitoring_views.baseline_fallback_summary AS
SELECT fallback_used,baseline_strategy,dataset_name,layer,severity,status,
       (observed_at_utc AT TIME ZONE 'UTC')::date AS observed_date_utc,
       count(*) AS evaluation_count,business_id
FROM monitoring_views.anomaly_baseline_history
GROUP BY fallback_used,baseline_strategy,dataset_name,layer,severity,status,business_id,
         (observed_at_utc AT TIME ZONE 'UTC')::date;

-- Grain: alert source/severity/status. Severity says urgency and status says lifecycle.
CREATE OR REPLACE VIEW monitoring_views.alert_summary_by_severity AS
SELECT source_type,severity,status,count(*) AS alert_count,max(created_at_utc) AS latest_alert_at_utc
FROM monitoring.alert_events
GROUP BY source_type,severity,status;

-- One incident instance. OFFSET 0 keeps operational views non-updatable.
CREATE OR REPLACE VIEW monitoring_views.alert_history AS
SELECT a.*,
       extract(epoch FROM (coalesce(resolved_at_utc,now())-first_seen_at_utc)) AS duration_seconds,
       (first_seen_at_utc AT TIME ZONE 'UTC')::date AS first_seen_date_utc,
       (last_seen_at_utc AT TIME ZONE 'UTC')::date AS last_seen_date_utc,
       (resolved_at_utc AT TIME ZONE 'UTC')::date AS resolved_date_utc,
       a.details->'dimensions'->>'business_id' AS business_id
FROM monitoring.alert_events a OFFSET 0;

CREATE OR REPLACE VIEW monitoring_views.active_alerts AS
SELECT * FROM monitoring_views.alert_history
WHERE lifecycle_status IN ('OPEN','ACKNOWLEDGED') OFFSET 0;

CREATE OR REPLACE VIEW monitoring_views.alert_summary_by_status AS
SELECT lifecycle_status,source_type,severity,dataset_name,layer,count(*) AS alert_count,
       sum(occurrence_count) AS occurrence_count,min(first_seen_at_utc) AS first_seen_at_utc,
       max(last_seen_at_utc) AS last_seen_at_utc,
       details->'dimensions'->>'business_id' AS business_id
FROM monitoring.alert_events GROUP BY lifecycle_status,source_type,severity,dataset_name,layer,
       details->'dimensions'->>'business_id';

CREATE OR REPLACE VIEW monitoring_views.recurring_alerts AS
SELECT * FROM monitoring_views.alert_history WHERE occurrence_count>1 OFFSET 0;

-- Grain: one physical provider attempt, enriched with safe routing context.
CREATE OR REPLACE VIEW monitoring_views.recent_deliveries AS
SELECT d.delivery_id,d.alert_event_id,d.logical_delivery_key,d.provider,d.destination_key,
       d.delivery_kind,d.delivery_version,d.escalation_level,d.delivery_status,
       d.attempted_at_utc,d.completed_at_utc,d.attempt_number,d.external_reference,
       d.error_message,d.details AS delivery_details,a.source_type,a.dataset_name,a.layer,
       a.severity,a.lifecycle_status,a.title,a.first_seen_at_utc,a.last_seen_at_utc,
       a.occurrence_count,(d.attempted_at_utc AT TIME ZONE 'UTC')::date AS attempted_date_utc,
       a.details->'dimensions'->>'business_id' AS business_id
FROM monitoring.alert_deliveries d
JOIN monitoring.alert_events a USING (alert_event_id)
OFFSET 0;

CREATE OR REPLACE VIEW monitoring_views.failed_deliveries AS
SELECT * FROM monitoring_views.recent_deliveries
WHERE delivery_status='FAILED' OFFSET 0;

-- Dimensions are retained so consumers can apply compatible filters before
-- rolling up provider status totals.
CREATE OR REPLACE VIEW monitoring_views.delivery_summary_by_provider AS
SELECT provider,destination_key,delivery_status,delivery_kind,dataset_name,layer,
       severity,lifecycle_status,attempted_date_utc,count(*) AS delivery_attempts,
       max(attempted_at_utc) AS latest_attempt_at_utc,business_id
FROM monitoring_views.recent_deliveries
GROUP BY provider,destination_key,delivery_status,delivery_kind,dataset_name,layer,business_id,
         severity,lifecycle_status,attempted_date_utc;

-- One active alert/provider pair with a successfully delivered escalation.
CREATE OR REPLACE VIEW monitoring_views.escalation_summary AS
SELECT a.alert_event_id,a.lifecycle_status,a.severity,a.dataset_name,a.layer,a.title,
       a.first_seen_at_utc,a.last_seen_at_utc,a.occurrence_count,d.provider,d.destination_key,
       max(d.escalation_level) AS escalation_level,max(d.completed_at_utc) AS escalated_at_utc,
       count(*) AS escalation_deliveries,a.details->'dimensions'->>'business_id' AS business_id
FROM monitoring.alert_events a
JOIN monitoring.alert_deliveries d USING (alert_event_id)
WHERE a.status IN ('OPEN','ACKNOWLEDGED') AND d.delivery_kind='ESCALATION'
  AND d.delivery_status='SENT'
GROUP BY a.alert_event_id,a.lifecycle_status,a.severity,a.dataset_name,a.layer,a.title,
         a.first_seen_at_utc,a.last_seen_at_utc,a.occurrence_count,d.provider,d.destination_key,
         a.details->'dimensions'->>'business_id';

-- Session callers may set pulse.monitoring_retention_days. Metabase and ordinary
-- SQL sessions receive the documented conservative 90-day default.
CREATE OR REPLACE VIEW monitoring_views.retention_eligible_counts AS
WITH settings AS (
    SELECT clock_timestamp() - make_interval(days =>
        coalesce(nullif(current_setting('pulse.monitoring_retention_days',true),''),'90')::integer
    ) AS cutoff_at_utc
), eligible_alerts AS (
    SELECT a.alert_event_id FROM monitoring.alert_events a,settings s
    WHERE a.status='RESOLVED' AND a.resolved_at_utc<s.cutoff_at_utc
), eligible_quality_runs AS (
    SELECT r.quality_run_id FROM monitoring.quality_runs r,settings s
    WHERE r.completed_at_utc<s.cutoff_at_utc AND NOT EXISTS (
        SELECT 1 FROM monitoring.quality_results q
        JOIN monitoring.alert_events a ON a.source_type='QUALITY_FAILURE'
                                      AND a.source_id=q.quality_result_id
        WHERE q.quality_run_id=r.quality_run_id
          AND NOT EXISTS (SELECT 1 FROM eligible_alerts e WHERE e.alert_event_id=a.alert_event_id)
    )
), eligible_anomalies AS (
    SELECT r.anomaly_id FROM monitoring.anomaly_results r,settings s
    WHERE r.evaluated_at_utc<s.cutoff_at_utc AND NOT EXISTS (
        SELECT 1 FROM monitoring.alert_events a WHERE a.source_type='ANOMALY'
          AND a.source_id=r.anomaly_id
          AND NOT EXISTS (SELECT 1 FROM eligible_alerts e WHERE e.alert_event_id=a.alert_event_id)
    )
)
SELECT 'alert_deliveries'::text AS relation_name,count(*)::bigint AS eligible_rows
FROM monitoring.alert_deliveries d WHERE EXISTS (
    SELECT 1 FROM eligible_alerts e WHERE e.alert_event_id=d.alert_event_id)
UNION ALL SELECT 'alert_event_history',count(*) FROM monitoring.alert_event_history h WHERE EXISTS (
    SELECT 1 FROM eligible_alerts e WHERE e.alert_event_id=h.alert_event_id)
UNION ALL SELECT 'alert_occurrences',count(*) FROM monitoring.alert_occurrences o WHERE EXISTS (
    SELECT 1 FROM eligible_alerts e WHERE e.alert_event_id=o.alert_event_id)
UNION ALL SELECT 'alert_events',count(*) FROM eligible_alerts
UNION ALL SELECT 'anomaly_results',count(*) FROM eligible_anomalies
UNION ALL SELECT 'quality_results',count(*) FROM monitoring.quality_results q WHERE EXISTS (
    SELECT 1 FROM eligible_quality_runs r WHERE r.quality_run_id=q.quality_run_id)
UNION ALL SELECT 'quality_runs',count(*) FROM eligible_quality_runs;
