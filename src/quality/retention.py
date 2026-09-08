"""Manual, transactional retention for monitoring history."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import os
from typing import Mapping

import psycopg

from src.quality.persistence import PersistenceError, ensure_monitoring_schema
from src.warehouse.load_gold import connection_kwargs


RETENTION_LOCK = 5357002
COUNT_ORDER = (
    "alert_deliveries", "alert_event_history", "alert_occurrences", "alert_events",
    "anomaly_results", "quality_results", "quality_runs",
)


class RetentionError(ValueError):
    """Safe configuration or operator-facing retention error."""


@dataclass(frozen=True)
class RetentionConfig:
    days: int = 90

    @classmethod
    def from_environ(cls, environ: Mapping[str, str] | None = None):
        environment = os.environ if environ is None else environ
        raw = environment.get("MONITORING_RETENTION_DAYS", "90")
        try:
            days = int(raw)
        except (TypeError, ValueError):
            raise RetentionError("MONITORING_RETENTION_DAYS must be an integer") from None
        if not 1 <= days <= 3650:
            raise RetentionError("MONITORING_RETENTION_DAYS must be between 1 and 3650")
        return cls(days=days)


@dataclass(frozen=True)
class RetentionReport:
    mode: str
    retention_days: int
    cutoff_at_utc: datetime
    counts: dict[str, int]

    @property
    def total_rows(self):
        return sum(self.counts.values())


def _eligible_ctes():
    return """WITH eligible_alerts AS (
        SELECT alert_event_id FROM monitoring.alert_events
        WHERE status='RESOLVED' AND resolved_at_utc < %(cutoff)s
    ), eligible_quality_runs AS (
        SELECT r.quality_run_id FROM monitoring.quality_runs r
        WHERE r.completed_at_utc < %(cutoff)s AND NOT EXISTS (
            SELECT 1 FROM monitoring.quality_results q
            JOIN monitoring.alert_events a ON a.source_type='QUALITY_FAILURE'
                                          AND a.source_id=q.quality_result_id
            WHERE q.quality_run_id=r.quality_run_id
              AND NOT EXISTS (SELECT 1 FROM eligible_alerts e WHERE e.alert_event_id=a.alert_event_id)
        )
    ), eligible_anomalies AS (
        SELECT r.anomaly_id FROM monitoring.anomaly_results r
        WHERE r.evaluated_at_utc < %(cutoff)s AND NOT EXISTS (
            SELECT 1 FROM monitoring.alert_events a WHERE a.source_type='ANOMALY'
              AND a.source_id=r.anomaly_id
              AND NOT EXISTS (SELECT 1 FROM eligible_alerts e WHERE e.alert_event_id=a.alert_event_id)
        )
    ) """


def _counts(cursor, cutoff):
    query = _eligible_ctes() + """
        SELECT 'alert_deliveries',count(*) FROM monitoring.alert_deliveries d
          WHERE EXISTS (SELECT 1 FROM eligible_alerts e WHERE e.alert_event_id=d.alert_event_id)
        UNION ALL SELECT 'alert_event_history',count(*) FROM monitoring.alert_event_history h
          WHERE EXISTS (SELECT 1 FROM eligible_alerts e WHERE e.alert_event_id=h.alert_event_id)
        UNION ALL SELECT 'alert_occurrences',count(*) FROM monitoring.alert_occurrences o
          WHERE EXISTS (SELECT 1 FROM eligible_alerts e WHERE e.alert_event_id=o.alert_event_id)
        UNION ALL SELECT 'alert_events',count(*) FROM eligible_alerts
        UNION ALL SELECT 'anomaly_results',count(*) FROM eligible_anomalies
        UNION ALL SELECT 'quality_results',count(*) FROM monitoring.quality_results q
          WHERE EXISTS (SELECT 1 FROM eligible_quality_runs r WHERE r.quality_run_id=q.quality_run_id)
        UNION ALL SELECT 'quality_runs',count(*) FROM eligible_quality_runs"""
    cursor.execute(query, {"cutoff": cutoff})
    found = dict(cursor.fetchall())
    return {name: found.get(name, 0) for name in COUNT_ORDER}


def preview_retention(*, config=None, now_utc=None):
    config = RetentionConfig.from_environ() if config is None else config
    now = datetime.now(timezone.utc) if now_utc is None else now_utc.astimezone(timezone.utc)
    cutoff = now - timedelta(days=config.days)
    try:
        with psycopg.connect(**connection_kwargs()) as connection:
            connection.execute("SET TRANSACTION READ ONLY")
            counts = _counts(connection.cursor(), cutoff)
    except Exception:
        raise PersistenceError("Retention preview failed; initialize monitoring and check warehouse access") from None
    return RetentionReport("preview", config.days, cutoff, counts)


def apply_retention(*, confirm=False, config=None, now_utc=None):
    if not confirm:
        raise RetentionError("Retention apply requires --confirm")
    config = RetentionConfig.from_environ() if config is None else config
    now = datetime.now(timezone.utc) if now_utc is None else now_utc.astimezone(timezone.utc)
    cutoff = now - timedelta(days=config.days)
    ensure_monitoring_schema()
    try:
        with psycopg.connect(**connection_kwargs()) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(%s)", (RETENTION_LOCK,))
                cursor.execute("""SELECT alert_event_id FROM monitoring.alert_events
                    WHERE status='RESOLVED' AND resolved_at_utc < %s
                    ORDER BY alert_event_id FOR UPDATE""", (cutoff,))
                counts = _counts(cursor, cutoff)
                deleted = {}
                params = {"cutoff": cutoff}
                cursor.execute("""DELETE FROM monitoring.alert_deliveries d USING monitoring.alert_events a
                    WHERE d.alert_event_id=a.alert_event_id AND a.status='RESOLVED'
                      AND a.resolved_at_utc < %(cutoff)s""", params)
                deleted["alert_deliveries"] = cursor.rowcount
                cursor.execute("""DELETE FROM monitoring.alert_event_history h USING monitoring.alert_events a
                    WHERE h.alert_event_id=a.alert_event_id AND a.status='RESOLVED'
                      AND a.resolved_at_utc < %(cutoff)s""", params)
                deleted["alert_event_history"] = cursor.rowcount
                cursor.execute("""DELETE FROM monitoring.alert_occurrences o USING monitoring.alert_events a
                    WHERE o.alert_event_id=a.alert_event_id AND a.status='RESOLVED'
                      AND a.resolved_at_utc < %(cutoff)s""", params)
                deleted["alert_occurrences"] = cursor.rowcount
                cursor.execute("""DELETE FROM monitoring.alert_events
                    WHERE status='RESOLVED' AND resolved_at_utc < %(cutoff)s""", params)
                deleted["alert_events"] = cursor.rowcount
                cursor.execute("""DELETE FROM monitoring.anomaly_results r
                    WHERE r.evaluated_at_utc < %(cutoff)s AND NOT EXISTS (
                        SELECT 1 FROM monitoring.alert_events a WHERE a.source_type='ANOMALY'
                          AND a.source_id=r.anomaly_id)""", params)
                deleted["anomaly_results"] = cursor.rowcount
                cursor.execute("""DELETE FROM monitoring.quality_results q USING monitoring.quality_runs r
                    WHERE q.quality_run_id=r.quality_run_id AND r.completed_at_utc < %(cutoff)s
                      AND NOT EXISTS (
                        SELECT 1 FROM monitoring.alert_events a
                        JOIN monitoring.quality_results source ON a.source_type='QUALITY_FAILURE'
                                                             AND source.quality_result_id=a.source_id
                        WHERE source.quality_run_id=r.quality_run_id)""", params)
                deleted["quality_results"] = cursor.rowcount
                cursor.execute("""DELETE FROM monitoring.quality_runs r
                    WHERE r.completed_at_utc < %(cutoff)s
                      AND NOT EXISTS (SELECT 1 FROM monitoring.quality_results q
                                      WHERE q.quality_run_id=r.quality_run_id)""", params)
                deleted["quality_runs"] = cursor.rowcount
                if deleted != counts:
                    raise RuntimeError("Retention eligibility changed during apply")
    except Exception:
        raise PersistenceError("Retention apply failed; no retention changes were committed") from None
    return RetentionReport("apply", config.days, cutoff, counts)
