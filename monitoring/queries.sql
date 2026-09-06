-- A. Latest status per layer, covering ALL datasets' latest completed attempts.
-- Display latest_check and dataset_count: missing/stale checks are not proof of health.
WITH latest AS (
    SELECT DISTINCT ON (dataset_name, layer) *
    FROM monitoring.quality_runs
    WHERE execution_source = 'airflow'
    ORDER BY dataset_name, layer, completed_at_utc DESC, quality_run_id
)
SELECT layer, count(*) AS dataset_count, max(completed_at_utc) AS latest_check,
       min(completed_at_utc) AS oldest_dataset_check,
       CASE WHEN bool_or(should_block) THEN 'FAIL'
            WHEN bool_or(overall_status = 'WARN') THEN 'WARN' ELSE 'PASS' END AS overall_status
FROM latest GROUP BY layer ORDER BY layer;

-- B. Daily warning/failure counts. Includes each actual attempt, but not duplicate writes.
SELECT date_trunc('day', completed_at_utc AT TIME ZONE 'UTC') AS day_utc, layer,
       sum(warning_checks) AS warnings, sum(failed_checks) AS failures,
       count(*) AS dataset_attempts
FROM monitoring.quality_runs
WHERE completed_at_utc >= now() - interval '30 days'
GROUP BY 1, 2 ORDER BY 1 DESC, 2;

-- C. Most frequently failing checks, scoped by dataset/layer.
SELECT r.dataset_name, r.layer, q.check_name, count(*) AS failures
FROM monitoring.quality_results q JOIN monitoring.quality_runs r USING (quality_run_id)
WHERE q.status = 'FAIL' AND r.completed_at_utc >= now() - interval '30 days'
GROUP BY 1, 2, 3 ORDER BY failures DESC LIMIT 20;

-- D. Recent critical failures with the Airflow attempt that produced them.
SELECT r.dag_id, r.airflow_run_id, r.task_id, r.attempt_number, r.dataset_name, r.layer,
       q.check_name, q.observed_value, q.expected_value, q.checked_at_utc
FROM monitoring.quality_results q JOIN monitoring.quality_runs r USING (quality_run_id)
WHERE q.severity = 'CRITICAL' AND q.status = 'FAIL'
ORDER BY q.checked_at_utc DESC LIMIT 50;

-- E. History for one dataset. Replace the literal or bind it as a query parameter.
SELECT quality_run_id, layer, execution_source, airflow_run_id, attempt_number,
       completed_at_utc, overall_status, passed_checks, warning_checks, failed_checks,
       extract(epoch FROM completed_at_utc - started_at_utc) AS duration_seconds
FROM monitoring.quality_runs WHERE dataset_name = 'daily_sales'
ORDER BY completed_at_utc DESC LIMIT 100;
