"""Atomic PostgreSQL persistence for one logical anomaly evaluation."""

from datetime import datetime, timezone

import psycopg
from psycopg.types.json import Jsonb

from src.quality.anomaly import AnomalyResult, AnomalyStatus
from src.quality.execution import ExecutionContext
from src.quality.persistence import PersistenceError, ensure_monitoring_schema, json_value
from src.warehouse.load_gold import connection_kwargs


def _result_details(result: AnomalyResult):
    """Make the result model authoritative for persisted contextual metadata."""
    return {**result.details,
            "baseline_strategy": result.baseline_strategy,
            "expected_value": result.expected_value,
            "lower_bound": result.lower_bound,
            "upper_bound": result.upper_bound,
            "trend_slope": result.trend_slope,
            "seasonal_reference_count": result.seasonal_reference_count,
            "training_window_size": result.training_window_size,
            "model_error": result.model_error,
            "residual": result.residual,
            "fallback_used": result.fallback_used,
            "confidence": result.confidence.value}


def persist_anomalies(results: list[AnomalyResult], context: ExecutionContext,
                      evaluated_at_utc: datetime | None = None):
    """Replace a logical evaluation across retries and retain incident lifecycle."""
    evaluated = datetime.now(timezone.utc) if evaluated_at_utc is None else evaluated_at_utc.astimezone(timezone.utc)
    evaluation_id = context.logical_id("pulse-anomaly-evaluation-v1")
    ensure_monitoring_schema()
    try:
        with psycopg.connect(**connection_kwargs()) as connection:
            with connection.cursor() as cursor:
                # Serialize replacements of the same logical evaluation, including empty retries.
                cursor.execute("SELECT pg_advisory_xact_lock(%s)",
                               (int.from_bytes(evaluation_id.bytes[:8], "big", signed=True),))
                cursor.execute("DELETE FROM monitoring.anomaly_results WHERE evaluation_id=%s", (evaluation_id,))
                rows = [(result.anomaly_id, evaluation_id, context.execution_source, context.execution_id,
                         context.dag_id, context.airflow_run_id, context.task_id, context.attempt_number,
                         context.map_index, context.logical_date_utc, result.metric_name, result.dataset_name,
                         result.layer, Jsonb(json_value(result.dimensions)), result.current_value,
                         result.baseline_value, result.deviation_value, result.deviation_percent,
                         Jsonb(json_value(result.threshold)), result.method, result.status.value,
                         result.severity.value, result.observed_at_utc, evaluated, result.history_count,
                         result.explanation, Jsonb(json_value(_result_details(result)))) for result in results]
                cursor.executemany("""INSERT INTO monitoring.anomaly_results (
                    anomaly_id,evaluation_id,execution_source,execution_id,dag_id,airflow_run_id,task_id,
                    attempt_number,map_index,logical_date_utc,metric_name,dataset_name,layer,dimensions,
                    current_value,baseline_value,deviation_value,deviation_percent,threshold,method,status,
                    severity,observed_at_utc,evaluated_at_utc,history_count,explanation,details)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", rows)
                from src.quality.alert_service import record_alert
                for result in sorted(results, key=lambda item: (item.dataset_name, item.layer,
                                     item.metric_name, str(sorted(item.dimensions.items())))):
                    if result.status == AnomalyStatus.ANOMALY:
                        record_alert(cursor,
                            occurrence_id=context.logical_id("pulse-anomaly-alert-v1", str(result.anomaly_id)),
                            source_type="ANOMALY", source_id=result.anomaly_id,
                            dataset_name=result.dataset_name, layer=result.layer, severity=result.severity.value,
                            title=f"Anomaly: {result.metric_name}", message=result.explanation,
                            seen_at=result.observed_at_utc, context=context, metric_name=result.metric_name,
                            dimensions=result.dimensions,
                            details={"method": result.method, "current_value": result.current_value,
                                     "baseline_value": result.baseline_value,
                                     "baseline_strategy": result.baseline_strategy,
                                     "confidence": result.confidence.value})
                # NORMAL and INSUFFICIENT_HISTORY never change lifecycle. Resolution is manual.
    except Exception:
        raise PersistenceError("Anomaly persistence failed; the evaluation transaction was rolled back") from None
    return evaluation_id
