"""Transactional alert lifecycle, independent of quality/anomaly calculation.

record_alert uses its caller's cursor so source persistence and alert changes
commit together. Operator commands own a transaction and never require Airflow.
"""

import hashlib
import json
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from src.warehouse.load_gold import connection_kwargs


STATES = ("OPEN", "ACKNOWLEDGED", "RESOLVED")


class AlertError(ValueError):
    """Safe operator-facing validation error."""


def incident_key(source_type, dataset_name, layer, metric_name, *, check_name=None, dimensions=None):
    """Versioned canonical JSON SHA-256, excluding severity/time/run/attempt."""
    if source_type not in ("QUALITY_FAILURE", "ANOMALY"):
        raise AlertError("Unknown alert source")
    if not all(isinstance(v, str) and v.strip() for v in (dataset_name, layer, metric_name)):
        raise AlertError("Dataset, layer and metric are required")
    if source_type == "QUALITY_FAILURE" and not check_name:
        raise AlertError("Quality incidents require a check name")
    payload = ["pulse-incident-v1", source_type, dataset_name, layer, metric_name,
               check_name if source_type == "QUALITY_FAILURE" else None, dimensions or {}]
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return "v1:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def validate_transition(previous, target):
    if (previous, target) not in (("OPEN", "ACKNOWLEDGED"), ("OPEN", "RESOLVED"),
                                 ("ACKNOWLEDGED", "RESOLVED")):
        raise AlertError(f"Invalid alert transition: {previous} -> {target}")


def _lock(cursor, key):
    # Same key always takes the same transaction lock, including creation races.
    cursor.execute("SELECT pg_advisory_xact_lock(%s)",
                   (int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], "big", signed=True),))


def _audit(cursor, identity, previous, target, actor, note=None):
    cursor.execute("""INSERT INTO monitoring.alert_event_history
        (alert_event_id,previous_status,new_status,changed_by,note)
        VALUES (%s,%s,%s,%s,%s)""", (identity, previous, target, actor, note))


def record_alert(cursor, *, occurrence_id, source_type, source_id, dataset_name, layer,
                 severity, title, message, seen_at, context, metric_name, check_name=None,
                 dimensions=None, details=None):
    """Open or update one active incident. Return its UUID, including on replay.

    A retry updates latest evidence but never changes status or counts. A replay
    associated with a resolved instance is a no-op. Severity is the highest seen
    within the instance, and timestamps use observation time, not retry time.
    """
    key = incident_key(source_type, dataset_name, layer, metric_name,
                       check_name=check_name, dimensions=dimensions)
    if severity not in ("WARNING", "CRITICAL") or seen_at.utcoffset() is None:
        raise AlertError("Alerts require WARNING/CRITICAL severity and a timezone-aware timestamp")
    _lock(cursor, key)
    cursor.execute("SELECT alert_event_id,incident_key FROM monitoring.alert_occurrences WHERE occurrence_id=%s",
                   (occurrence_id,))
    receipt = cursor.fetchone()
    if receipt and receipt[1] != key:
        raise AlertError("Occurrence identity belongs to a different incident")
    if receipt:
        identity = receipt[0]
        cursor.execute("SELECT status FROM monitoring.alert_events WHERE alert_event_id=%s FOR UPDATE", (identity,))
        if cursor.fetchone()[0] == "RESOLVED":
            return identity
    else:
        cursor.execute("SELECT alert_event_id FROM monitoring.alert_events "
                       "WHERE incident_key=%s AND status IN ('OPEN','ACKNOWLEDGED') FOR UPDATE", (key,))
        active = cursor.fetchone()
        identity = active[0] if active else uuid4()
    evidence = {**(details or {}), "metric_name": metric_name, "dimensions": dimensions or {}}
    if check_name is not None:
        evidence["check_name"] = check_name
    execution = (context.execution_source, context.execution_id, context.dag_id, context.airflow_run_id,
                 context.task_id, context.attempt_number, context.map_index, context.logical_date_utc)
    if not receipt and not active:
        cursor.execute("""INSERT INTO monitoring.alert_events (
            alert_event_id,source_type,source_id,dataset_name,layer,severity,status,title,message,
            created_at_utc,execution_source,execution_id,dag_id,airflow_run_id,task_id,attempt_number,
            map_index,logical_date_utc,details,incident_key,first_seen_at_utc,last_seen_at_utc)
            VALUES (%s,%s,%s,%s,%s,%s,'OPEN',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (identity, source_type, source_id, dataset_name, layer, severity, title, message,
             seen_at, *execution, Jsonb(evidence), key, seen_at, seen_at))
        _audit(cursor, identity, None, "OPEN", "system:alert-service", "Condition detected")
    else:
        # Latest evidence follows observation time. Older arrivals count but do
        # not overwrite the latest source/execution. Severity never decreases.
        cursor.execute("""UPDATE monitoring.alert_events SET
            source_id=CASE WHEN %s>=last_seen_at_utc THEN %s ELSE source_id END,
            title=CASE WHEN %s>=last_seen_at_utc THEN %s ELSE title END,
            message=CASE WHEN %s>=last_seen_at_utc THEN %s ELSE message END,
            details=CASE WHEN %s>=last_seen_at_utc THEN %s ELSE details END,
            execution_source=CASE WHEN %s>=last_seen_at_utc THEN %s ELSE execution_source END,
            execution_id=CASE WHEN %s>=last_seen_at_utc THEN %s ELSE execution_id END,
            dag_id=CASE WHEN %s>=last_seen_at_utc THEN %s ELSE dag_id END,
            airflow_run_id=CASE WHEN %s>=last_seen_at_utc THEN %s ELSE airflow_run_id END,
            task_id=CASE WHEN %s>=last_seen_at_utc THEN %s ELSE task_id END,
            attempt_number=CASE WHEN %s>=last_seen_at_utc THEN %s ELSE attempt_number END,
            map_index=CASE WHEN %s>=last_seen_at_utc THEN %s ELSE map_index END,
            logical_date_utc=CASE WHEN %s>=last_seen_at_utc THEN %s ELSE logical_date_utc END,
            severity=CASE WHEN severity='CRITICAL' OR %s='CRITICAL' THEN 'CRITICAL' ELSE 'WARNING' END,
            first_seen_at_utc=LEAST(first_seen_at_utc,%s),
            last_seen_at_utc=GREATEST(last_seen_at_utc,%s),
            occurrence_count=occurrence_count+%s WHERE alert_event_id=%s""",
            (seen_at, source_id, seen_at, title, seen_at, message, seen_at, Jsonb(evidence),
             *(part for value in execution for part in (seen_at, value)),
             severity, seen_at, seen_at, 0 if receipt else 1, identity))
    if not receipt:
        cursor.execute("""INSERT INTO monitoring.alert_occurrences
            (occurrence_id,alert_event_id,incident_key,observed_at_utc,source_id,snapshot)
            VALUES (%s,%s,%s,%s,%s,%s)""",
            (occurrence_id, identity, key, seen_at, source_id,
             Jsonb({"details": evidence, "execution_id": context.execution_id,
                    "execution_source": context.execution_source, "severity": severity})))
    return identity


def change_status(alert_id, target, *, by, note=None):
    """Strict transitions with an atomic audit write. Repeated commands reject."""
    identity = UUID(str(alert_id))
    if not isinstance(by, str) or not by.strip():
        raise AlertError("--by must contain an operator name")
    if target not in ("ACKNOWLEDGED", "RESOLVED"):
        raise AlertError("Target must be ACKNOWLEDGED or RESOLVED")
    from src.quality.persistence import PersistenceError, ensure_monitoring_schema
    ensure_monitoring_schema()
    try:
        with psycopg.connect(**connection_kwargs()) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT incident_key FROM monitoring.alert_events WHERE alert_event_id=%s", (identity,))
                row = cursor.fetchone()
                if row is None:
                    raise AlertError("Alert not found")
                _lock(cursor, row[0])
                cursor.execute("SELECT status FROM monitoring.alert_events WHERE alert_event_id=%s FOR UPDATE", (identity,))
                previous = cursor.fetchone()[0]
                validate_transition(previous, target)
                if target == "ACKNOWLEDGED":
                    cursor.execute("UPDATE monitoring.alert_events SET status=%s,acknowledged_at_utc=clock_timestamp(),"
                                   "acknowledged_by=%s WHERE alert_event_id=%s", (target, by.strip(), identity))
                else:
                    cursor.execute("UPDATE monitoring.alert_events SET status=%s,resolved_at_utc=clock_timestamp(),"
                                   "resolved_by=%s,resolution_note=%s WHERE alert_event_id=%s",
                                   (target, by.strip(), note, identity))
                _audit(cursor, identity, previous, target, by.strip(), note)
    except AlertError:
        raise
    except Exception:
        raise PersistenceError("Alert transition failed; state and audit were rolled back") from None
    return identity


def acknowledge_alert(alert_id, *, by):
    return change_status(alert_id, "ACKNOWLEDGED", by=by)


def resolve_alert(alert_id, *, by, note=None):
    return change_status(alert_id, "RESOLVED", by=by, note=note)


def list_alerts(*, status=None, dataset=None, layer=None, severity=None, limit=100):
    """Active by default, status='ALL' includes history. No schema writes."""
    if status not in (None, "ALL", *STATES) or not 1 <= limit <= 1000:
        raise AlertError("Invalid status or limit (1..1000)")
    from src.quality.persistence import PersistenceError
    try:
        with psycopg.connect(**connection_kwargs(), row_factory=dict_row) as connection:
            connection.execute("SET TRANSACTION READ ONLY")
            return connection.execute("""SELECT * FROM monitoring.alert_events
                WHERE (%s::text='ALL' OR (%s::text IS NULL AND status IN ('OPEN','ACKNOWLEDGED')) OR status=%s)
                AND (%s::text IS NULL OR dataset_name=%s) AND (%s::text IS NULL OR layer=%s)
                AND (%s::text IS NULL OR severity=%s)
                ORDER BY last_seen_at_utc DESC,alert_event_id LIMIT %s""",
                (status, status, status, dataset, dataset, layer, layer, severity, severity, limit)).fetchall()
    except Exception:
        raise PersistenceError("Alert query failed; initialize monitoring and check warehouse access") from None
