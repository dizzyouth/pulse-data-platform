"""Optional PostgreSQL sink; calculation modules never import this adapter."""

from dataclasses import asdict
from datetime import datetime
import json
from pathlib import Path
from uuid import uuid5

import psycopg
from psycopg.types.json import Jsonb

from src.quality.execution import DatasetQualityRun, ExecutionContext
from src.quality.models import summarize
from src.warehouse.load_gold import connection_kwargs


class PersistenceError(RuntimeError):
    """Safe public error: do not log database diagnostics or connection settings."""


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Unsupported JSON value type: {type(value).__name__}")


def json_value(value):
    # Validate before starting a transaction; retain numbers, booleans, objects,
    # arrays and JSON null rather than coercing observations to text.
    return json.loads(json.dumps(value, default=_json_default, allow_nan=False))


def ensure_monitoring_schema() -> None:
    """Repeatable initialization, serialized to avoid concurrent CREATE races."""
    try:
        with psycopg.connect(**connection_kwargs()) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(5353001)")
                for statement in Path(__file__).with_name("monitoring.sql").read_text(encoding="utf-8").split(";"):
                    if statement.strip():
                        cursor.execute(statement)
    except Exception:
        raise PersistenceError("Monitoring initialization failed; check warehouse availability and permissions") from None


def persist_quality_run(run: DatasetQualityRun, context: ExecutionContext):
    """Commit one dataset and all checks, or preserve the previous complete version.

    UUID identity includes the Airflow attempt. Rewrites of the same attempt
    replace only its checks. The upsert locks the parent row before replacement,
    serializing concurrent writers of the same identity.
    """
    run_id = context.run_id(run.dataset_name, run.layer)
    summary = summarize(run.results)
    rows = [
        (uuid5(run_id, r.check_name), run_id, r.check_name, r.metric_name, r.status.value, r.severity.value,
         Jsonb(json_value(r.observed_value)), Jsonb(json_value(r.expected_value)), r.checked_at_utc,
         Jsonb(json_value(asdict(r.details)))) for r in run.results
    ]
    ensure_monitoring_schema()
    try:
        with psycopg.connect(**connection_kwargs()) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO monitoring.quality_runs (
                        quality_run_id, execution_source, execution_id, dag_id, airflow_run_id, task_id,
                        attempt_number, map_index, logical_date_utc, dataset_name, layer,
                        started_at_utc, completed_at_utc, overall_status, total_checks, passed_checks,
                        warning_checks, failed_checks, critical_failures, should_block
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (quality_run_id) DO UPDATE SET
                        started_at_utc = EXCLUDED.started_at_utc,
                        completed_at_utc = EXCLUDED.completed_at_utc,
                        logical_date_utc = EXCLUDED.logical_date_utc,
                        overall_status = EXCLUDED.overall_status,
                        total_checks = EXCLUDED.total_checks, passed_checks = EXCLUDED.passed_checks,
                        warning_checks = EXCLUDED.warning_checks, failed_checks = EXCLUDED.failed_checks,
                        critical_failures = EXCLUDED.critical_failures, should_block = EXCLUDED.should_block
                    """, (run_id, context.execution_source, context.execution_id, context.dag_id,
                          context.airflow_run_id, context.task_id, context.attempt_number, context.map_index,
                          context.logical_date_utc, run.dataset_name, run.layer, run.started_at_utc,
                          run.completed_at_utc, summary.overall_status.value, summary.total_checks,
                          summary.passed, summary.warnings, summary.failed, summary.critical_failures,
                          summary.critical_failures > 0))
                cursor.execute("DELETE FROM monitoring.quality_results WHERE quality_run_id = %s", (run_id,))
                cursor.executemany("""
                    INSERT INTO monitoring.quality_results (
                        quality_result_id, quality_run_id, check_name, metric_name, status, severity,
                        observed_value, expected_value, checked_at_utc, details
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, rows)
    except Exception:
        raise PersistenceError("Quality persistence failed; the dataset transaction was rolled back") from None
    return run_id


if __name__ == "__main__":
    ensure_monitoring_schema()
    print("Monitoring schema is ready")
