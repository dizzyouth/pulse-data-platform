"""Phase 5.7 delivery/retention contracts and isolated PostgreSQL tests."""

from __future__ import annotations

from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import io
import logging
import os
import unittest
from unittest.mock import patch
from uuid import uuid4

import psycopg
from psycopg import sql

from src.quality.alert_service import acknowledge_alert, record_alert, resolve_alert
from src.quality.anomaly import AnomalyResult, AnomalyStatus
from src.quality.anomaly_persistence import persist_anomalies
from src.quality.delivery import (
    DeliveryConfig,
    DeliveryError,
    deliver_due,
    logical_delivery_key,
    retry_delivery,
)
from src.quality.delivery_cli import main as delivery_main
from src.quality.execution import ExecutionContext
from src.quality.models import Severity, Status
from src.quality.notifications import (
    AlertNotification,
    DeliveryContext,
    DeliveryResult,
    LoggingProvider,
)
from src.quality.persistence import ensure_monitoring_schema, persist_quality_run
from src.quality.retention import (
    RetentionConfig,
    RetentionError,
    apply_retention,
    preview_retention,
)
from src.quality.retention_cli import main as retention_main
from src.warehouse.load_gold import connection_kwargs
from src.warehouse.monitoring import ensure_monitoring_views
from tests.test_quality import sample_result
from tests.test_quality_persistence import fixture_run


class RecordingProvider:
    name = "log"

    def __init__(self, outcomes=None):
        self.outcomes = list(outcomes or [])
        self.calls = []

    def send(self, alert, context):
        self.calls.append((alert, context))
        if self.outcomes:
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        return DeliveryResult("SENT", external_reference=f"local:{len(self.calls)}")


class DeliveryContractsTests(unittest.TestCase):
    def test_default_log_provider_succeeds_without_network(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        alert = AlertNotification(uuid4(), "WARNING", "OPEN", "Fixture", "Details",
                                  "daily_sales", "analytics", now, now, 1)
        context = DeliveryContext("v1:key", "INITIAL", 1, 0, "local-log", 1, now)
        with self.assertLogs("pulse.alert_delivery", logging.INFO) as captured:
            result = LoggingProvider().send(alert, context)
        self.assertEqual(result.status, "SENT")
        self.assertIn(str(alert.alert_event_id), captured.output[0])
        self.assertNotIn(alert.message, captured.output[0])

    def test_delivery_key_is_deterministic_and_route_version_sensitive(self):
        identity = uuid4()
        key = logical_delivery_key(identity, "log", "local-log", "INITIAL", 1)
        self.assertEqual(key, logical_delivery_key(identity, "log", "local-log", "INITIAL", 1))
        self.assertNotEqual(key, logical_delivery_key(identity, "log", "other", "INITIAL", 1))
        self.assertNotEqual(key, logical_delivery_key(identity, "log", "local-log", "ESCALATION", 1))

    def test_configuration_is_bounded_and_recurrence_is_opt_in(self):
        config = DeliveryConfig.from_environ({})
        self.assertEqual((config.provider, config.max_attempts, config.recurrence_threshold), ("log", 3, 0))
        with self.assertRaises(DeliveryError):
            DeliveryConfig.from_environ({"ALERT_RECURRENCE_THRESHOLD": "1"})
        with self.assertRaises(DeliveryError):
            DeliveryConfig.from_environ({"ALERT_DELIVERY_PROVIDER": "webhook"})

    def test_clis_keep_provider_failure_nonfatal_and_require_retention_confirmation(self):
        with patch("src.quality.delivery_cli.deliver_due") as sweep, \
             patch("sys.stdout", new_callable=io.StringIO):
            from src.quality.delivery import DeliveryRunSummary
            sweep.return_value = DeliveryRunSummary(attempted=1, failed=1)
            self.assertEqual(delivery_main(["sweep"]), 0)
        with patch("src.quality.retention_cli.apply_retention", side_effect=RetentionError("requires --confirm")), \
             patch("sys.stderr", new_callable=io.StringIO):
            self.assertEqual(retention_main(["apply"]), 1)


@unittest.skipUnless(os.environ.get("RUN_MONITORING_INTEGRATION_TESTS") == "1",
                     "Requires disposable PostgreSQL database; set RUN_MONITORING_INTEGRATION_TESTS=1")
class DeliveryRetentionPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = connection_kwargs()
        cls.database = "pulse_delivery_test_" + uuid4().hex
        with psycopg.connect(**cls.base, autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cls.database)))
        cls.addClassCleanup(cls.cleanup_database)
        cls.config = {**cls.base, "dbname": cls.database}
        for module in (
            "src.quality.persistence", "src.quality.anomaly_persistence", "src.quality.alert_service",
            "src.quality.delivery", "src.quality.retention", "src.warehouse.monitoring",
        ):
            override = patch(module + ".connection_kwargs", return_value=cls.config)
            override.start()
            cls.addClassCleanup(override.stop)
        ensure_monitoring_schema()
        ensure_monitoring_views()

    @classmethod
    def cleanup_database(cls):
        with psycopg.connect(**cls.base, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(cls.database)))

    def setUp(self):
        with psycopg.connect(**self.config) as connection:
            connection.execute("TRUNCATE monitoring.alert_deliveries,monitoring.alert_event_history,"
                               "monitoring.alert_occurrences,monitoring.alert_events,"
                               "monitoring.anomaly_results,monitoring.quality_results,monitoring.quality_runs")
        self.now = datetime(2026, 4, 10, 12, tzinfo=timezone.utc)

    def fetch(self, query, params=()):
        with psycopg.connect(**self.config) as connection:
            return connection.execute(query, params).fetchall()

    def record(self, execution="first", *, severity="WARNING", seen_at=None, dimensions=None):
        context = ExecutionContext(execution_id=execution)
        with psycopg.connect(**self.config) as connection:
            return record_alert(
                connection.cursor(), occurrence_id=context.logical_id("delivery-fixture", dimensions or {}),
                source_type="ANOMALY", source_id=context.logical_id("source"),
                dataset_name="fixture", layer="analytics", severity=severity,
                title="Synthetic alert", message="Synthetic details", seen_at=seen_at or self.now,
                context=context, metric_name="revenue", dimensions=dimensions,
            )

    def test_warning_initial_is_idempotent_and_resolved_never_delivers(self):
        provider = RecordingProvider()
        identity = self.record()
        first = deliver_due(provider=provider, now_utc=self.now)
        second = deliver_due(provider=provider, now_utc=self.now + timedelta(minutes=10))
        self.assertEqual((first.sent, second.attempted, len(provider.calls)), (1, 0, 1))
        self.assertEqual(self.fetch("SELECT delivery_kind,delivery_status,attempt_number FROM monitoring.alert_deliveries"),
                         [("INITIAL", "SENT", 1)])
        other = self.record("resolved", dimensions={"currency": "EUR"})
        resolve_alert(other, by="operator")
        deliver_due(provider=provider, now_utc=self.now + timedelta(minutes=20))
        self.assertEqual(len(provider.calls), 1)
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.alert_deliveries WHERE alert_event_id=%s", (other,)), [(0,)])
        self.assertEqual(self.fetch("SELECT status FROM monitoring.alert_events WHERE alert_event_id=%s", (identity,)), [("OPEN",)])

    def test_concurrent_sweeps_claim_once_and_delivered_alerts_do_not_starve_limit(self):
        provider = RecordingProvider()
        self.record("old", seen_at=self.now - timedelta(minutes=1), dimensions={"currency": "USD"})
        deliver_due(provider=provider, now_utc=self.now, limit=1)
        self.record("new", dimensions={"currency": "EUR"})
        deliver_due(provider=provider, now_utc=self.now, limit=1)
        self.assertEqual(len(provider.calls), 2)
        third = self.record("third", seen_at=self.now + timedelta(minutes=1), dimensions={"currency": "GBP"})
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: deliver_due(provider=provider, now_utc=self.now + timedelta(minutes=1)), range(4)))
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.alert_deliveries WHERE alert_event_id=%s", (third,)), [(1,)])

    def test_failure_is_persisted_then_bounded_retry_succeeds_without_duplicate_logical_delivery(self):
        provider = RecordingProvider([RuntimeError("https://secret.example/token"), DeliveryResult("SENT")])
        self.record()
        config = DeliveryConfig(retry_delay_minutes=5, max_attempts=2)
        failed = deliver_due(config=config, provider=provider, now_utc=self.now)
        early = deliver_due(config=config, provider=provider, now_utc=self.now + timedelta(minutes=4))
        sent = deliver_due(config=config, provider=provider, now_utc=self.now + timedelta(minutes=5))
        self.assertEqual((failed.failed, early.attempted, sent.sent), (1, 0, 1))
        rows = self.fetch("SELECT attempt_number,delivery_status,error_message,logical_delivery_key "
                          "FROM monitoring.alert_deliveries ORDER BY attempt_number")
        self.assertEqual([(r[0], r[1]) for r in rows], [(1, "FAILED"), (2, "SENT")])
        self.assertEqual(len({r[3] for r in rows}), 1)
        self.assertNotIn("secret", rows[0][2])
        with self.assertRaises(DeliveryError):
            retry_delivery(self.fetch("SELECT delivery_id FROM monitoring.alert_deliveries ORDER BY attempt_number")[0][0],
                           config=config, provider=provider, now_utc=self.now + timedelta(minutes=6))

    def test_explicit_retry_bypasses_delay_but_not_attempt_bound(self):
        provider = RecordingProvider([RuntimeError("offline"), RuntimeError("offline")])
        self.record()
        config = DeliveryConfig(retry_delay_minutes=60, max_attempts=2)
        deliver_due(config=config, provider=provider, now_utc=self.now)
        first_id = self.fetch("SELECT delivery_id FROM monitoring.alert_deliveries")[0][0]
        self.assertEqual(retry_delivery(first_id, config=config, provider=provider,
                                        now_utc=self.now + timedelta(minutes=1))["retry_status"], "FAILED")
        second_id = self.fetch("SELECT delivery_id FROM monitoring.alert_deliveries ORDER BY attempt_number DESC")[0][0]
        with self.assertRaises(DeliveryError):
            retry_delivery(second_id, config=config, provider=provider,
                           now_utc=self.now + timedelta(minutes=2))

    def test_acknowledged_recurrence_requires_enabled_threshold(self):
        provider = RecordingProvider()
        identity = self.record()
        deliver_due(provider=provider, now_utc=self.now)
        acknowledge_alert(identity, by="operator")
        self.record("second", seen_at=self.now + timedelta(minutes=1))
        deliver_due(provider=provider, now_utc=self.now + timedelta(minutes=2))
        self.assertEqual(len(provider.calls), 1)
        config = DeliveryConfig(recurrence_threshold=2)
        deliver_due(config=config, provider=provider, now_utc=self.now + timedelta(minutes=3))
        deliver_due(config=config, provider=provider, now_utc=self.now + timedelta(minutes=4))
        self.assertEqual(len(provider.calls), 2)
        self.assertEqual(self.fetch("SELECT delivery_kind FROM monitoring.alert_deliveries ORDER BY attempted_at_utc"),
                         [("INITIAL",), ("RECURRENCE",)])

    def test_critical_escalates_once_only_after_threshold_while_open(self):
        provider = RecordingProvider()
        self.record(severity="CRITICAL", seen_at=self.now - timedelta(hours=2))
        config = DeliveryConfig(critical_escalation_minutes=60)
        deliver_due(config=config, provider=provider, now_utc=self.now)
        self.assertEqual([call[1].delivery_kind for call in provider.calls], ["INITIAL"])
        deliver_due(config=config, provider=provider, now_utc=self.now + timedelta(minutes=1))
        deliver_due(config=config, provider=provider, now_utc=self.now + timedelta(minutes=2))
        self.assertEqual([call[1].delivery_kind for call in provider.calls], ["INITIAL", "ESCALATION"])
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.alert_deliveries WHERE delivery_kind='ESCALATION'"), [(1,)])

    def anomaly(self, execution, status, severity, observed, dimensions):
        context = ExecutionContext(execution_id=execution)
        result = AnomalyResult(
            anomaly_id=context.logical_id("retention-anomaly", dimensions), metric_name="gross_revenue",
            dataset_name="daily_sales", layer="analytics", dimensions=dimensions,
            current_value=200, baseline_value=100, deviation_value=100, deviation_percent=100,
            threshold={"warning_ratio": 0.5}, method="percentage_deviation", status=status,
            severity=severity, observed_at_utc=observed, explanation="Synthetic retention fixture",
            history_count=7, details={},
        )
        persist_anomalies([result], context, evaluated_at_utc=observed)
        return result.anomaly_id

    def quality(self, execution, completed, *, alert=False):
        base = sample_result(Status.FAIL, Severity.CRITICAL) if alert else sample_result()
        result = replace(base, checked_at_utc=completed,
                         dataset_name="retention_fixture", layer="gold")
        run = replace(fixture_run(), dataset_name="retention_fixture", layer="gold",
                      started_at_utc=completed - timedelta(seconds=1), completed_at_utc=completed,
                      results=(result,))
        return persist_quality_run(run, ExecutionContext(execution_id=execution))

    def test_retention_preview_apply_preserves_active_sources_and_integrity(self):
        old = self.now - timedelta(days=120)
        self.anomaly("normal-old", AnomalyStatus.NORMAL, Severity.INFO, old, {"currency": "GBP"})
        active_source = self.anomaly("active-old", AnomalyStatus.ANOMALY, Severity.WARNING, old, {"currency": "USD"})
        self.anomaly("resolved-old", AnomalyStatus.ANOMALY, Severity.WARNING, old, {"currency": "EUR"})
        resolved = self.fetch("SELECT alert_event_id FROM monitoring.alert_events "
                              "WHERE source_id<>(%s) ORDER BY alert_event_id", (active_source,))[0][0]
        deliver_due(provider=RecordingProvider(), now_utc=old)
        resolve_alert(resolved, by="retention-test")
        with psycopg.connect(**self.config) as connection:
            connection.execute("UPDATE monitoring.alert_events SET resolved_at_utc=%s WHERE alert_event_id=%s",
                               (old, resolved))
            connection.execute("UPDATE monitoring.alert_event_history SET changed_at_utc=%s WHERE alert_event_id=%s",
                               (old, resolved))
        self.quality("quality-old", old)
        active_quality_run = self.quality("quality-active-old", old + timedelta(minutes=1), alert=True)
        active_quality_source = self.fetch(
            "SELECT quality_result_id FROM monitoring.quality_results WHERE quality_run_id=%s",
            (active_quality_run,),
        )[0][0]
        self.quality("quality-recent", self.now - timedelta(days=10))
        before = self.fetch("SELECT (SELECT count(*) FROM monitoring.alert_events),"
                            "(SELECT count(*) FROM monitoring.anomaly_results),"
                            "(SELECT count(*) FROM monitoring.quality_runs)")[0]
        report = preview_retention(config=RetentionConfig(90), now_utc=self.now)
        self.assertEqual(report.counts["alert_events"], 1)
        self.assertEqual(report.counts["anomaly_results"], 2)
        self.assertEqual((report.counts["quality_runs"], report.counts["quality_results"]), (1, 1))
        self.assertEqual(self.fetch("SELECT (SELECT count(*) FROM monitoring.alert_events),"
                                    "(SELECT count(*) FROM monitoring.anomaly_results),"
                                    "(SELECT count(*) FROM monitoring.quality_runs)")[0], before)
        with self.assertRaises(RetentionError):
            apply_retention(config=RetentionConfig(90), now_utc=self.now)
        applied = apply_retention(confirm=True, config=RetentionConfig(90), now_utc=self.now)
        self.assertEqual(applied.counts, report.counts)
        self.assertEqual(set(self.fetch("SELECT source_id,status FROM monitoring.alert_events")),
                         {(active_source, "OPEN"), (active_quality_source, "OPEN")})
        self.assertEqual(self.fetch("SELECT anomaly_id FROM monitoring.anomaly_results"), [(active_source,)])
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.quality_runs"), [(2,)])
        self.assertEqual(preview_retention(config=RetentionConfig(90), now_utc=self.now).total_rows, 0)

    def test_delivery_views_are_read_only_and_expose_failed_and_escalated_state(self):
        provider = RecordingProvider()
        self.record(severity="CRITICAL", seen_at=self.now - timedelta(hours=2))
        config = DeliveryConfig(critical_escalation_minutes=60)
        deliver_due(config=config, provider=provider, now_utc=self.now)
        deliver_due(config=config, provider=provider, now_utc=self.now + timedelta(minutes=1))
        expected = ("recent_deliveries", "failed_deliveries", "delivery_summary_by_provider",
                    "escalation_summary", "retention_eligible_counts")
        for view in expected:
            self.assertEqual(self.fetch("SELECT is_updatable FROM information_schema.views "
                                        "WHERE table_schema='monitoring_views' AND table_name=%s", (view,)), [("NO",)])
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring_views.escalation_summary"), [(1,)])
        self.assertEqual(self.fetch("SELECT sum(delivery_attempts) FROM monitoring_views.delivery_summary_by_provider"), [(2,)])
        self.assertEqual({r[0] for r in self.fetch("SELECT relation_name FROM monitoring_views.retention_eligible_counts")},
                         {"alert_deliveries", "alert_event_history", "alert_occurrences", "alert_events",
                          "anomaly_results", "quality_results", "quality_runs"})


if __name__ == "__main__":
    unittest.main()
