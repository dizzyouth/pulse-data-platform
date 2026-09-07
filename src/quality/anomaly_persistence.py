"""Atomic PostgreSQL persistence for one logical anomaly evaluation."""

from datetime import datetime, timezone

import psycopg
from psycopg.types.json import Jsonb

from src.quality.anomaly import AnomalyResult, AnomalyStatus
from src.quality.execution import ExecutionContext
from src.quality.persistence import PersistenceError, ensure_monitoring_schema, json_value
from src.warehouse.load_gold import connection_kwargs


def persist_anomalies(results: list[AnomalyResult], context: ExecutionContext,
                      evaluated_at_utc: datetime | None = None):
    """Replace a logical evaluation across retries and atomically rebuild its alerts."""
    evaluated = datetime.now(timezone.utc) if evaluated_at_utc is None else evaluated_at_utc.astimezone(timezone.utc)
    evaluation_id = context.logical_id("pulse-anomaly-evaluation-v1")
    ensure_monitoring_schema()
    try:
        with psycopg.connect(**connection_kwargs()) as connection:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM monitoring.alert_events WHERE source_type='ANOMALY' AND source_id IN "
                               "(SELECT anomaly_id FROM monitoring.anomaly_results WHERE evaluation_id=%s)",
                               (evaluation_id,))
                cursor.execute("DELETE FROM monitoring.anomaly_results WHERE evaluation_id=%s", (evaluation_id,))
                rows = [(result.anomaly_id, evaluation_id, context.execution_source, context.execution_id,
                         context.dag_id, context.airflow_run_id, context.task_id, context.attempt_number,
                         context.map_index, context.logical_date_utc, result.metric_name, result.dataset_name,
                         result.layer, Jsonb(json_value(result.dimensions)), result.current_value,
                         result.baseline_value, result.deviation_value, result.deviation_percent,
                         Jsonb(json_value(result.threshold)), result.method, result.status.value,
                         result.severity.value, result.observed_at_utc, evaluated, result.history_count,
                         result.explanation, Jsonb(json_value(result.details))) for result in results]
                cursor.executemany("""INSERT INTO monitoring.anomaly_results (
                    anomaly_id,evaluation_id,execution_source,execution_id,dag_id,airflow_run_id,task_id,
                    attempt_number,map_index,logical_date_utc,metric_name,dataset_name,layer,dimensions,
                    current_value,baseline_value,deviation_value,deviation_percent,threshold,method,status,
                    severity,observed_at_utc,evaluated_at_utc,history_count,explanation,details)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", rows)
                alerts = []
                for result in results:
                    if result.status == AnomalyStatus.ANOMALY:
                        event_id = context.logical_id("pulse-anomaly-alert-v1", str(result.anomaly_id))
                        alerts.append((event_id, "ANOMALY", result.anomaly_id, result.dataset_name,
                                       result.layer, result.severity.value, "OPEN",
                                       f"{result.severity.value.title()} anomaly: {result.metric_name}",
                                       result.explanation, evaluated, context.execution_source,
                                       context.execution_id, context.dag_id, context.airflow_run_id,
                                       context.task_id, context.attempt_number, context.map_index,
                                       context.logical_date_utc, Jsonb({"dimensions": result.dimensions,
                                                                       "method": result.method})))
                cursor.executemany("""INSERT INTO monitoring.alert_events (
                    alert_event_id,source_type,source_id,dataset_name,layer,severity,status,title,message,
                    created_at_utc,execution_source,execution_id,dag_id,airflow_run_id,task_id,
                    attempt_number,map_index,logical_date_utc,details)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", alerts)
    except Exception:
        raise PersistenceError("Anomaly persistence failed; the evaluation transaction was rolled back") from None
    return evaluation_id
