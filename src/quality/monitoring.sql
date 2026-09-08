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

CREATE TABLE IF NOT EXISTS monitoring.anomaly_results (
    anomaly_id UUID PRIMARY KEY,
    evaluation_id UUID NOT NULL,
    execution_source TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    dag_id TEXT,
    airflow_run_id TEXT,
    task_id TEXT,
    attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
    map_index INTEGER NOT NULL DEFAULT -1 CHECK (map_index >= -1),
    logical_date_utc TIMESTAMPTZ,
    metric_name TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    layer TEXT NOT NULL,
    dimensions JSONB NOT NULL DEFAULT '{}'::jsonb,
    current_value DOUBLE PRECISION NOT NULL,
    baseline_value DOUBLE PRECISION,
    deviation_value DOUBLE PRECISION,
    deviation_percent DOUBLE PRECISION,
    threshold JSONB NOT NULL,
    method TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('NORMAL', 'ANOMALY', 'INSUFFICIENT_HISTORY')),
    severity TEXT NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL')),
    observed_at_utc TIMESTAMPTZ NOT NULL,
    evaluated_at_utc TIMESTAMPTZ NOT NULL,
    history_count INTEGER NOT NULL CHECK (history_count >= 0),
    explanation TEXT NOT NULL,
    details JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS monitoring.alert_events (
    alert_event_id UUID PRIMARY KEY,
    source_type TEXT NOT NULL CHECK (source_type IN ('QUALITY_FAILURE', 'ANOMALY')),
    source_id UUID NOT NULL,
    dataset_name TEXT NOT NULL,
    layer TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('WARNING', 'CRITICAL')),
    status TEXT NOT NULL CHECK (status = 'OPEN'),
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL,
    execution_source TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    dag_id TEXT,
    airflow_run_id TEXT,
    task_id TEXT,
    attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
    map_index INTEGER NOT NULL DEFAULT -1 CHECK (map_index >= -1),
    logical_date_utc TIMESTAMPTZ,
    details JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS quality_runs_completed_idx ON monitoring.quality_runs(completed_at_utc DESC);
CREATE INDEX IF NOT EXISTS quality_runs_dataset_idx ON monitoring.quality_runs(dataset_name, layer, completed_at_utc DESC);
CREATE INDEX IF NOT EXISTS quality_runs_layer_idx ON monitoring.quality_runs(layer, completed_at_utc DESC);
CREATE INDEX IF NOT EXISTS quality_runs_status_idx ON monitoring.quality_runs(overall_status, completed_at_utc DESC);
CREATE INDEX IF NOT EXISTS quality_results_critical_idx ON monitoring.quality_results(checked_at_utc DESC)
    WHERE severity = 'CRITICAL' AND status = 'FAIL';
CREATE INDEX IF NOT EXISTS anomaly_results_observed_idx ON monitoring.anomaly_results(observed_at_utc DESC);
CREATE INDEX IF NOT EXISTS anomaly_results_dataset_idx ON monitoring.anomaly_results(dataset_name, layer, observed_at_utc DESC);
CREATE INDEX IF NOT EXISTS anomaly_results_flagged_idx ON monitoring.anomaly_results(severity, observed_at_utc DESC)
    WHERE status = 'ANOMALY';
CREATE INDEX IF NOT EXISTS alert_events_created_idx ON monitoring.alert_events(created_at_utc DESC);
CREATE INDEX IF NOT EXISTS alert_events_severity_idx ON monitoring.alert_events(severity, status, created_at_utc DESC);

-- Phase 5.6 additive migration. The initializer backfills legacy rows before
-- installing the unique active-incident index in the same transaction.
ALTER TABLE monitoring.alert_events DROP CONSTRAINT IF EXISTS alert_events_status_check;
ALTER TABLE monitoring.alert_events ADD CONSTRAINT alert_events_status_check
    CHECK (status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED'));
ALTER TABLE monitoring.alert_events
    ADD COLUMN IF NOT EXISTS lifecycle_status TEXT GENERATED ALWAYS AS (status) STORED,
    ADD COLUMN IF NOT EXISTS incident_key TEXT,
    ADD COLUMN IF NOT EXISTS first_seen_at_utc TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_seen_at_utc TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS occurrence_count BIGINT NOT NULL DEFAULT 1 CHECK (occurrence_count >= 1),
    ADD COLUMN IF NOT EXISTS acknowledged_at_utc TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS resolved_at_utc TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS acknowledged_by TEXT,
    ADD COLUMN IF NOT EXISTS resolved_by TEXT,
    ADD COLUMN IF NOT EXISTS resolution_note TEXT;

CREATE TABLE IF NOT EXISTS monitoring.alert_event_history (
    history_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    alert_event_id UUID NOT NULL REFERENCES monitoring.alert_events(alert_event_id),
    previous_status TEXT CHECK (previous_status IN ('OPEN', 'ACKNOWLEDGED')),
    new_status TEXT NOT NULL CHECK (new_status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')),
    changed_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    changed_by TEXT NOT NULL CHECK (length(trim(changed_by)) > 0),
    note TEXT,
    CHECK ((previous_status IS NULL AND new_status='OPEN') OR
           (previous_status='OPEN' AND new_status IN ('ACKNOWLEDGED','RESOLVED')) OR
           (previous_status='ACKNOWLEDGED' AND new_status='RESOLVED'))
);
CREATE INDEX IF NOT EXISTS alert_history_event_idx
    ON monitoring.alert_event_history(alert_event_id, history_id);

-- One receipt per logical execution/check or evaluation/series, excluding retry.
-- Receipts survive resolution so replay cannot create a fresh incident.
CREATE TABLE IF NOT EXISTS monitoring.alert_occurrences (
    occurrence_id UUID PRIMARY KEY,
    alert_event_id UUID NOT NULL REFERENCES monitoring.alert_events(alert_event_id),
    incident_key TEXT NOT NULL,
    observed_at_utc TIMESTAMPTZ NOT NULL,
    source_id UUID NOT NULL,
    snapshot JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS alert_occurrences_event_idx
    ON monitoring.alert_occurrences(alert_event_id);

-- Phase 5.7 delivery outbox. A logical notification may have several bounded
-- physical attempts, while the versioned logical key prevents duplicate work.
CREATE TABLE IF NOT EXISTS monitoring.alert_deliveries (
    delivery_id UUID PRIMARY KEY,
    alert_event_id UUID NOT NULL REFERENCES monitoring.alert_events(alert_event_id),
    logical_delivery_key TEXT NOT NULL,
    provider TEXT NOT NULL CHECK (length(trim(provider)) > 0),
    destination_key TEXT NOT NULL CHECK (length(trim(destination_key)) > 0),
    delivery_kind TEXT NOT NULL CHECK (delivery_kind IN ('INITIAL', 'RECURRENCE', 'ESCALATION')),
    delivery_version INTEGER NOT NULL CHECK (delivery_version >= 1),
    escalation_level INTEGER NOT NULL DEFAULT 0 CHECK (escalation_level >= 0),
    delivery_status TEXT NOT NULL CHECK (delivery_status IN ('PENDING', 'SENT', 'FAILED', 'SKIPPED')),
    attempted_at_utc TIMESTAMPTZ NOT NULL,
    completed_at_utc TIMESTAMPTZ,
    attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
    external_reference TEXT,
    error_message TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (logical_delivery_key, attempt_number),
    CHECK ((delivery_status = 'PENDING' AND completed_at_utc IS NULL) OR
           (delivery_status <> 'PENDING' AND completed_at_utc IS NOT NULL)),
    CHECK ((delivery_kind = 'ESCALATION' AND escalation_level >= 1) OR
           (delivery_kind <> 'ESCALATION' AND escalation_level = 0))
);
CREATE INDEX IF NOT EXISTS alert_deliveries_event_idx
    ON monitoring.alert_deliveries(alert_event_id, attempted_at_utc DESC);
CREATE INDEX IF NOT EXISTS alert_deliveries_status_idx
    ON monitoring.alert_deliveries(delivery_status, attempted_at_utc DESC);
CREATE INDEX IF NOT EXISTS alert_deliveries_logical_idx
    ON monitoring.alert_deliveries(logical_delivery_key, attempt_number DESC);
