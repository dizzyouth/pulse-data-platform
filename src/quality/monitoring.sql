CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.quality_runs (
    quality_run_id UUID PRIMARY KEY,
    execution_source TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    dag_id TEXT,
    airflow_run_id TEXT,
    task_id TEXT,
    attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
    map_index INTEGER NOT NULL DEFAULT -1 CHECK (map_index >= -1),
    logical_date_utc TIMESTAMPTZ,
    dataset_name TEXT NOT NULL,
    layer TEXT NOT NULL,
    started_at_utc TIMESTAMPTZ NOT NULL,
    completed_at_utc TIMESTAMPTZ NOT NULL,
    overall_status TEXT NOT NULL CHECK (overall_status IN ('PASS', 'WARN', 'FAIL')),
    total_checks INTEGER NOT NULL CHECK (total_checks >= 0),
    passed_checks INTEGER NOT NULL CHECK (passed_checks >= 0),
    warning_checks INTEGER NOT NULL CHECK (warning_checks >= 0),
    failed_checks INTEGER NOT NULL CHECK (failed_checks >= 0),
    critical_failures INTEGER NOT NULL CHECK (critical_failures >= 0 AND critical_failures <= failed_checks),
    should_block BOOLEAN NOT NULL,
    CHECK (completed_at_utc >= started_at_utc),
    CHECK (total_checks = passed_checks + warning_checks + failed_checks),
    CHECK (should_block = (critical_failures > 0)),
    CHECK (overall_status = CASE WHEN critical_failures > 0 THEN 'FAIL'
        WHEN warning_checks + failed_checks > 0 THEN 'WARN' ELSE 'PASS' END)
);

CREATE TABLE IF NOT EXISTS monitoring.quality_results (
    quality_result_id UUID PRIMARY KEY,
    quality_run_id UUID NOT NULL REFERENCES monitoring.quality_runs(quality_run_id),
    check_name TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PASS', 'WARN', 'FAIL')),
    severity TEXT NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL')),
    observed_value JSONB NOT NULL,
    expected_value JSONB NOT NULL,
    checked_at_utc TIMESTAMPTZ NOT NULL,
    details JSONB NOT NULL,
    UNIQUE (quality_run_id, check_name)
);

CREATE INDEX IF NOT EXISTS quality_runs_completed_idx ON monitoring.quality_runs(completed_at_utc DESC);
CREATE INDEX IF NOT EXISTS quality_runs_dataset_idx ON monitoring.quality_runs(dataset_name, layer, completed_at_utc DESC);
CREATE INDEX IF NOT EXISTS quality_runs_layer_idx ON monitoring.quality_runs(layer, completed_at_utc DESC);
CREATE INDEX IF NOT EXISTS quality_runs_status_idx ON monitoring.quality_runs(overall_status, completed_at_utc DESC);
CREATE INDEX IF NOT EXISTS quality_results_critical_idx ON monitoring.quality_results(checked_at_utc DESC)
    WHERE severity = 'CRITICAL' AND status = 'FAIL';
