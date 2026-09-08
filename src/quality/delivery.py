"""PostgreSQL-backed alert delivery, retry, recurrence, and escalation policy."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from typing import Mapping
from uuid import UUID, uuid5

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from src.quality.notifications import (
    AlertNotification,
    DeliveryContext,
    DeliveryResult,
    LoggingProvider,
    NotificationProvider,
)
from src.quality.persistence import PersistenceError, ensure_monitoring_schema, json_value
from src.warehouse.load_gold import connection_kwargs


DELIVERY_NAMESPACE = UUID("fe8d2827-cef9-50e0-98a2-4e9f734dd31d")
DELIVERY_KINDS = ("INITIAL", "RECURRENCE", "ESCALATION")


class DeliveryError(ValueError):
    """Safe configuration or operator-facing delivery error."""


def _integer(environment, name, default, *, minimum, maximum):
    raw = environment.get(name, str(default))
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise DeliveryError(f"{name} must be an integer") from None
    if not minimum <= value <= maximum:
        raise DeliveryError(f"{name} must be between {minimum} and {maximum}")
    return value


@dataclass(frozen=True)
class DeliveryConfig:
    provider: str = "log"
    destination_key: str = "local-log"
    max_attempts: int = 3
    retry_delay_minutes: int = 5
    critical_escalation_minutes: int = 60
    recurrence_threshold: int = 0

    @classmethod
    def from_environ(cls, environ: Mapping[str, str] | None = None):
        environment = os.environ if environ is None else environ
        provider = environment.get("ALERT_DELIVERY_PROVIDER", "log").strip().lower()
        destination = environment.get("ALERT_DELIVERY_DESTINATION_KEY", "local-log").strip()
        if provider != "log":
            raise DeliveryError("ALERT_DELIVERY_PROVIDER must be 'log' in Phase 5.7")
        if not destination:
            raise DeliveryError("ALERT_DELIVERY_DESTINATION_KEY must not be blank")
        recurrence = _integer(environment, "ALERT_RECURRENCE_THRESHOLD", 0, minimum=0, maximum=1_000_000)
        if recurrence == 1:
            raise DeliveryError("ALERT_RECURRENCE_THRESHOLD must be 0 (disabled) or at least 2")
        return cls(
            provider=provider,
            destination_key=destination,
            max_attempts=_integer(environment, "ALERT_DELIVERY_MAX_ATTEMPTS", 3, minimum=1, maximum=10),
            retry_delay_minutes=_integer(environment, "ALERT_DELIVERY_RETRY_MINUTES", 5, minimum=0, maximum=1440),
            critical_escalation_minutes=_integer(
                environment, "ALERT_CRITICAL_ESCALATION_MINUTES", 60, minimum=0, maximum=43200
            ),
            recurrence_threshold=recurrence,
        )


@dataclass(frozen=True)
class DeliveryCandidate:
    alert: AlertNotification
    provider: str
    destination_key: str
    delivery_kind: str
    delivery_version: int
    escalation_level: int = 0

    @property
    def logical_key(self):
        return logical_delivery_key(
            self.alert.alert_event_id, self.provider, self.destination_key,
            self.delivery_kind, self.delivery_version,
        )


@dataclass(frozen=True)
class DeliveryRunSummary:
    candidates: int = 0
    attempted: int = 0
    sent: int = 0
    failed: int = 0
    skipped: int = 0
    deferred: int = 0


def logical_delivery_key(alert_event_id, provider, destination_key, delivery_kind, delivery_version):
    if delivery_kind not in DELIVERY_KINDS:
        raise DeliveryError("Unknown delivery kind")
    payload = ["pulse-delivery-v1", str(UUID(str(alert_event_id))), provider,
               destination_key, delivery_kind, int(delivery_version)]
    canonical = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    return "v1:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _provider(config, provider=None):
    selected = LoggingProvider() if provider is None else provider
    if selected.name != config.provider:
        raise DeliveryError(f"Configured provider '{config.provider}' is unavailable")
    return selected


def _notification(row):
    return AlertNotification(
        alert_event_id=row["alert_event_id"], severity=row["severity"],
        lifecycle_status=row["status"], title=row["title"], message=row["message"],
        dataset_name=row["dataset_name"], layer=row["layer"],
        first_seen_at_utc=row["first_seen_at_utc"], last_seen_at_utc=row["last_seen_at_utc"],
        occurrence_count=row["occurrence_count"],
    )


def _candidate_is_still_due(candidate, row, config, now):
    if row["status"] == "RESOLVED":
        return False
    if candidate.delivery_kind == "INITIAL":
        return row["status"] == "OPEN"
    if candidate.delivery_kind == "RECURRENCE":
        return (row["status"] in ("OPEN", "ACKNOWLEDGED") and
                config.recurrence_threshold >= 2 and
                row["occurrence_count"] >= candidate.delivery_version)
    return (row["status"] == "OPEN" and row["severity"] == "CRITICAL" and
            config.critical_escalation_minutes > 0 and
            row["first_seen_at_utc"] <= now - timedelta(minutes=config.critical_escalation_minutes))


def _discover_candidates(connection, config, now, limit):
    rows = connection.execute("""WITH enriched AS (SELECT a.*,
        initial.delivery_status AS initial_status,
        initial.attempt_number AS initial_attempt,
        initial.completed_at_utc AS initial_completed,
        recurrence.delivery_status AS recurrence_status,
        recurrence.attempt_number AS recurrence_attempt,
        recurrence.completed_at_utc AS recurrence_completed,
        escalation.delivery_status AS escalation_status,
        escalation.attempt_number AS escalation_attempt,
        escalation.completed_at_utc AS escalation_completed
        FROM monitoring.alert_events a
        LEFT JOIN LATERAL (SELECT d.delivery_status,d.attempt_number,d.completed_at_utc
            FROM monitoring.alert_deliveries d
            WHERE d.alert_event_id=a.alert_event_id AND d.provider=%(provider)s
              AND d.destination_key=%(destination)s
              AND d.delivery_kind='INITIAL' AND d.delivery_version=1
            ORDER BY d.attempt_number DESC LIMIT 1) initial ON true
        LEFT JOIN LATERAL (SELECT d.delivery_status,d.attempt_number,d.completed_at_utc
            FROM monitoring.alert_deliveries d
            WHERE d.alert_event_id=a.alert_event_id AND d.provider=%(provider)s
              AND d.destination_key=%(destination)s
              AND d.delivery_kind='RECURRENCE' AND d.delivery_version=%(recurrence)s
            ORDER BY d.attempt_number DESC LIMIT 1) recurrence ON true
        LEFT JOIN LATERAL (SELECT d.delivery_status,d.attempt_number,d.completed_at_utc
            FROM monitoring.alert_deliveries d
            WHERE d.alert_event_id=a.alert_event_id AND d.provider=%(provider)s
              AND d.destination_key=%(destination)s
              AND d.delivery_kind='ESCALATION' AND d.delivery_version=1
            ORDER BY d.attempt_number DESC LIMIT 1) escalation ON true
        WHERE a.status IN ('OPEN','ACKNOWLEDGED')
    ), due AS (SELECT e.*,
        e.status='OPEN' AND (e.initial_status IS NULL OR
            (e.initial_status='FAILED' AND e.initial_attempt<%(max_attempts)s
             AND e.initial_completed<=%(retry_cutoff)s)) AS initial_due,
        %(recurrence)s>=2 AND e.occurrence_count>=%(recurrence)s AND
            (e.recurrence_status IS NULL OR
             (e.recurrence_status='FAILED' AND e.recurrence_attempt<%(max_attempts)s
              AND e.recurrence_completed<=%(retry_cutoff)s)) AS recurrence_due,
        %(escalation_minutes)s>0 AND e.status='OPEN' AND e.severity='CRITICAL'
            AND e.initial_status='SENT'
            AND e.first_seen_at_utc<=%(escalation_cutoff)s AND
            (e.escalation_status IS NULL OR
             (e.escalation_status='FAILED' AND e.escalation_attempt<%(max_attempts)s
              AND e.escalation_completed<=%(retry_cutoff)s)) AS escalation_due
        FROM enriched e)
        SELECT * FROM due WHERE initial_due OR recurrence_due OR escalation_due
        ORDER BY first_seen_at_utc,alert_event_id LIMIT %(limit)s""", {
            "provider": config.provider, "destination": config.destination_key,
            "recurrence": config.recurrence_threshold, "max_attempts": config.max_attempts,
            "retry_cutoff": now - timedelta(minutes=config.retry_delay_minutes),
            "escalation_cutoff": now - timedelta(minutes=config.critical_escalation_minutes),
            "escalation_minutes": config.critical_escalation_minutes,
            "limit": limit,
        }).fetchall()
    candidates = []
    for row in rows:
        alert = _notification(row)
        if row["initial_due"]:
            candidates.append(DeliveryCandidate(alert, config.provider, config.destination_key, "INITIAL", 1))
        if row["recurrence_due"]:
            candidates.append(DeliveryCandidate(
                alert, config.provider, config.destination_key, "RECURRENCE", config.recurrence_threshold
            ))
        if config.critical_escalation_minutes > 0 and row["escalation_due"]:
            candidates.append(DeliveryCandidate(
                alert, config.provider, config.destination_key, "ESCALATION", 1, escalation_level=1
            ))
    return candidates


def _claim(candidate, config, now, *, force=False):
    key = candidate.logical_key
    try:
        with psycopg.connect(**connection_kwargs(), row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(%s)", (
                    int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], "big", signed=True),
                ))
                cursor.execute("SELECT * FROM monitoring.alert_events WHERE alert_event_id=%s FOR UPDATE",
                               (candidate.alert.alert_event_id,))
                alert_row = cursor.fetchone()
                if alert_row is None or not _candidate_is_still_due(candidate, alert_row, config, now):
                    return None
                cursor.execute("""SELECT * FROM monitoring.alert_deliveries
                    WHERE logical_delivery_key=%s ORDER BY attempt_number DESC LIMIT 1""", (key,))
                previous = cursor.fetchone()
                if previous and previous["delivery_status"] in ("PENDING", "SENT", "SKIPPED"):
                    return None
                attempt = 1 if previous is None else previous["attempt_number"] + 1
                if attempt > config.max_attempts:
                    return None
                if (previous and not force and
                        previous["completed_at_utc"] + timedelta(minutes=config.retry_delay_minutes) > now):
                    return None
                delivery_id = uuid5(DELIVERY_NAMESPACE, f"{key}:{attempt}")
                cursor.execute("""INSERT INTO monitoring.alert_deliveries (
                    delivery_id,alert_event_id,logical_delivery_key,provider,destination_key,
                    delivery_kind,delivery_version,escalation_level,delivery_status,
                    attempted_at_utc,attempt_number,details)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'PENDING',%s,%s,%s)""",
                    (delivery_id, candidate.alert.alert_event_id, key, candidate.provider,
                     candidate.destination_key, candidate.delivery_kind, candidate.delivery_version,
                     candidate.escalation_level, now, attempt,
                     Jsonb({"policy": "pulse-delivery-v1"})))
        return delivery_id, attempt
    except Exception:
        raise PersistenceError("Delivery claim failed; check monitoring storage") from None


def _complete(delivery_id, result, completed_at):
    try:
        details = json_value(dict(result.details))
        with psycopg.connect(**connection_kwargs()) as connection:
            updated = connection.execute("""UPDATE monitoring.alert_deliveries SET
                delivery_status=%s,completed_at_utc=%s,external_reference=%s,
                error_message=%s,details=details || %s
                WHERE delivery_id=%s AND delivery_status='PENDING'""",
                (result.status, completed_at, result.external_reference, result.error_message,
                 Jsonb({"provider_result": details}), delivery_id)).rowcount
            if updated != 1:
                raise RuntimeError("Delivery attempt was not pending")
    except Exception:
        raise PersistenceError("Delivery completion could not be persisted") from None


def _attempt(candidate, config, provider, now, *, force=False):
    claimed = _claim(candidate, config, now, force=force)
    if claimed is None:
        return None
    delivery_id, attempt_number = claimed
    context = DeliveryContext(
        logical_delivery_key=candidate.logical_key, delivery_kind=candidate.delivery_kind,
        delivery_version=candidate.delivery_version, escalation_level=candidate.escalation_level,
        destination_key=candidate.destination_key, attempt_number=attempt_number,
        attempted_at_utc=now,
    )
    try:
        result = provider.send(candidate.alert, context)
        if not isinstance(result, DeliveryResult):
            raise TypeError("Invalid provider result")
    except Exception as error:
        # Provider exception text can contain URLs, credentials, or payloads.
        result = DeliveryResult(
            status="FAILED", error_message=f"Provider delivery failed ({type(error).__name__})",
            details={"exception_type": type(error).__name__},
        )
    _complete(delivery_id, result, datetime.now(timezone.utc) if now is None else now)
    return result.status


def deliver_due(*, config=None, provider=None, now_utc=None, limit=100):
    """Attempt due notifications. Provider failures are returned, never raised."""
    if not 1 <= limit <= 1000:
        raise DeliveryError("limit must be between 1 and 1000")
    config = DeliveryConfig.from_environ() if config is None else config
    selected = _provider(config, provider)
    now = datetime.now(timezone.utc) if now_utc is None else now_utc.astimezone(timezone.utc)
    ensure_monitoring_schema()
    try:
        with psycopg.connect(**connection_kwargs(), row_factory=dict_row) as connection:
            connection.execute("SET TRANSACTION READ ONLY")
            candidates = _discover_candidates(connection, config, now, limit)
    except Exception:
        raise PersistenceError("Delivery candidates could not be read") from None
    counts = {"attempted": 0, "sent": 0, "failed": 0, "skipped": 0, "deferred": 0}
    for candidate in candidates:
        status = _attempt(candidate, config, selected, now)
        if status is None:
            counts["deferred"] += 1
        else:
            counts["attempted"] += 1
            counts[status.lower()] += 1
    return DeliveryRunSummary(candidates=len(candidates), **counts)


def list_deliveries(*, statuses=("PENDING", "FAILED"), limit=100):
    if not statuses or any(s not in ("PENDING", "SENT", "FAILED", "SKIPPED") for s in statuses):
        raise DeliveryError("Invalid delivery status")
    if not 1 <= limit <= 1000:
        raise DeliveryError("limit must be between 1 and 1000")
    try:
        with psycopg.connect(**connection_kwargs(), row_factory=dict_row) as connection:
            connection.execute("SET TRANSACTION READ ONLY")
            return connection.execute("""SELECT * FROM monitoring.alert_deliveries
                WHERE delivery_status=ANY(%s) ORDER BY attempted_at_utc,delivery_id LIMIT %s""",
                (list(statuses), limit)).fetchall()
    except Exception:
        raise PersistenceError("Delivery attempts could not be read") from None


def retry_delivery(delivery_id, *, config=None, provider=None, now_utc=None):
    """Immediately retry the latest FAILED attempt for one logical delivery."""
    identity = UUID(str(delivery_id))
    config = DeliveryConfig.from_environ() if config is None else config
    selected = _provider(config, provider)
    now = datetime.now(timezone.utc) if now_utc is None else now_utc.astimezone(timezone.utc)
    ensure_monitoring_schema()
    try:
        with psycopg.connect(**connection_kwargs(), row_factory=dict_row) as connection:
            connection.execute("SET TRANSACTION READ ONLY")
            row = connection.execute("""SELECT d.*,a.* FROM monitoring.alert_deliveries d
                JOIN monitoring.alert_events a USING (alert_event_id) WHERE d.delivery_id=%s""",
                (identity,)).fetchone()
            if row is None:
                raise DeliveryError("Delivery not found")
            latest = connection.execute("""SELECT delivery_id,delivery_status FROM monitoring.alert_deliveries
                WHERE logical_delivery_key=%s ORDER BY attempt_number DESC LIMIT 1""",
                (row["logical_delivery_key"],)).fetchone()
    except DeliveryError:
        raise
    except Exception:
        raise PersistenceError("Delivery retry state could not be read") from None
    if latest["delivery_id"] != identity or latest["delivery_status"] != "FAILED":
        raise DeliveryError("Only the latest FAILED delivery attempt can be retried")
    candidate = DeliveryCandidate(
        _notification(row), row["provider"], row["destination_key"], row["delivery_kind"],
        row["delivery_version"], row["escalation_level"],
    )
    status = _attempt(candidate, config, selected, now, force=True)
    if status is None:
        raise DeliveryError("Delivery is no longer eligible or has reached its retry limit")
    return {"delivery_id": str(identity), "retry_status": status}
