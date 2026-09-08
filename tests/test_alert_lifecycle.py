"""Deterministic lifecycle contracts and disposable PostgreSQL acceptance tests."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import io
import os
from pathlib import Path
import re
import unittest
from unittest.mock import patch
from uuid import uuid4

import psycopg
from psycopg import sql

from bi.monitoring_dashboard import question_query
from src.quality.alert_cli import main
from src.quality.alert_service import (
    AlertError, acknowledge_alert, incident_key, list_alerts, record_alert,
    resolve_alert, validate_transition,
)
from src.quality.anomaly import AnomalyStatus
from src.quality.anomaly_persistence import persist_anomalies
from src.quality.execution import ExecutionContext
from src.quality.models import Severity, Status
from src.quality.persistence import PersistenceError, ensure_monitoring_schema, persist_quality_run
from src.warehouse.load_gold import connection_kwargs
from src.warehouse.monitoring import ensure_monitoring_views
from tests import test_anomaly_postgres
from tests.test_quality import sample_result
from tests.test_quality_persistence import fixture_run


class AlertContractTests(unittest.TestCase):
    def test_fingerprint_canonical_and_dimension_source_check_sensitive(self):
        def key(**kwargs):
            return incident_key("ANOMALY", "sales", "analytics", "revenue", **kwargs)
        self.assertEqual(key(dimensions={"currency": "USD", "country": "US"}),
                         key(dimensions={"country": "US", "currency": "USD"}))
        self.assertNotEqual(key(dimensions={"currency": "USD"}), key(dimensions={"currency": "EUR"}))
        self.assertNotEqual(key(), incident_key("QUALITY_FAILURE", "sales", "analytics", "revenue", check_name="c"))
        self.assertNotEqual(incident_key("QUALITY_FAILURE", "sales", "gold", "count", check_name="a"),
                            incident_key("QUALITY_FAILURE", "sales", "gold", "count", check_name="b"))
        with self.assertRaises(AlertError):
            incident_key("QUALITY_FAILURE", "sales", "gold", "count")

    def test_all_transition_pairs(self):
        valid = {("OPEN", "ACKNOWLEDGED"), ("OPEN", "RESOLVED"), ("ACKNOWLEDGED", "RESOLVED")}
        for before in ("OPEN", "ACKNOWLEDGED", "RESOLVED"):
            for after in ("OPEN", "ACKNOWLEDGED", "RESOLVED"):
                if (before, after) in valid:
                    validate_transition(before, after)
                else:
                    with self.assertRaises(AlertError):
                        validate_transition(before, after)

    def test_cli_empty_list_success_and_errors(self):
        with patch("src.quality.alert_cli.list_alerts", return_value=[]) as query, \
             patch("sys.stdout", new_callable=io.StringIO) as output:
            self.assertEqual(main(["list"]), 0)
            self.assertEqual(output.getvalue().strip(), "[]")
            self.assertIsNone(query.call_args.kwargs["status"])
        with patch("src.quality.alert_cli.acknowledge_alert", side_effect=AlertError("Alert not found")), \
             patch("sys.stderr", new_callable=io.StringIO) as error:
            self.assertEqual(main(["acknowledge", str(uuid4()), "--by", "operator"]), 1)
            self.assertIn("Alert not found", error.getvalue())

    def test_quality_writes_checks_before_shared_service(self):
        order = []
        with patch("src.quality.persistence.ensure_monitoring_schema"), \
             patch("src.quality.persistence.psycopg.connect") as connect, \
             patch("src.quality.alert_service.record_alert", side_effect=lambda *a, **k: order.append("alert")):
            cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
            cursor.executemany.side_effect = lambda *a: order.append("results")
            persist_quality_run(fixture_run(sample_result(Status.FAIL, Severity.CRITICAL)),
                                ExecutionContext(execution_id="ordering"))
        self.assertEqual(order, ["results", "alert"])


@unittest.skipUnless(os.environ.get("RUN_MONITORING_INTEGRATION_TESTS") == "1",
                     "Requires disposable PostgreSQL database; set RUN_MONITORING_INTEGRATION_TESTS=1")
class AlertPostgresTests(unittest.TestCase):
    anomaly = test_anomaly_postgres.AnomalyPostgresTests.anomaly

    @classmethod
    def setUpClass(cls):
        cls.base = connection_kwargs()
        cls.database = "pulse_alert_test_" + uuid4().hex
        with psycopg.connect(**cls.base, autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cls.database)))
        cls.addClassCleanup(cls.cleanup_database)
        cls.config = {**cls.base, "dbname": cls.database}
        for module in ("src.quality.persistence", "src.quality.anomaly_persistence",
                       "src.quality.alert_service", "src.warehouse.monitoring"):
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
            connection.execute("TRUNCATE monitoring.alert_deliveries,monitoring.alert_event_history,monitoring.alert_occurrences,"
                               "monitoring.alert_events,monitoring.anomaly_results,"
                               "monitoring.quality_results,monitoring.quality_runs")
        self.time = datetime(2026, 1, 8, tzinfo=timezone.utc)

    def fetch(self, statement, params=()):
        with psycopg.connect(**self.config) as connection:
            return connection.execute(statement, params).fetchall()

    def record(self, execution="first", *, time=None, attempt=1, dimensions=None, severity="WARNING"):
        context = ExecutionContext(execution_id=execution, attempt_number=attempt)
        with psycopg.connect(**self.config) as connection:
            with connection.cursor() as cursor:
                return record_alert(cursor, occurrence_id=context.logical_id("receipt", dimensions or {}),
                    source_type="ANOMALY", source_id=context.logical_id("source"), dataset_name="fixture",
                    layer="analytics", severity=severity, title="Fixture", message="Synthetic condition",
                    seen_at=time or self.time, context=context, metric_name="revenue", dimensions=dimensions)

    def test_open_recurrence_acknowledgement_resolution_and_new_instance(self):
        identity = self.record()
        self.assertEqual(self.fetch("SELECT status,lifecycle_status,occurrence_count FROM monitoring.alert_events"),
                         [("OPEN", "OPEN", 1)])
        self.assertEqual(self.record("second", time=self.time+timedelta(days=1)), identity)
        acknowledge_alert(identity, by="alice")
        self.record("third", time=self.time+timedelta(days=2), severity="CRITICAL")
        row = list_alerts()[0]
        self.assertEqual((row["status"], row["occurrence_count"], row["acknowledged_by"], row["severity"]),
                         ("ACKNOWLEDGED", 3, "alice", "CRITICAL"))
        self.assertEqual(row["first_seen_at_utc"], self.time)
        self.assertEqual(row["last_seen_at_utc"], self.time+timedelta(days=2))
        self.assertIsNotNone(row["acknowledged_at_utc"])
        resolve_alert(identity, by="bob", note="Validated recovery")
        self.assertEqual(list_alerts(), [])
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring_views.active_alerts"), [(0,)])
        row = list_alerts(status="RESOLVED")[0]
        self.assertEqual((row["resolved_by"], row["resolution_note"]), ("bob", "Validated recovery"))
        self.assertIsNotNone(row["resolved_at_utc"])
        self.assertEqual(self.fetch("SELECT previous_status,new_status,changed_by FROM monitoring.alert_event_history "
                                   "ORDER BY history_id"),
                         [(None,"OPEN","system:alert-service"),("OPEN","ACKNOWLEDGED","alice"),
                          ("ACKNOWLEDGED","RESOLVED","bob")])
        self.assertEqual(self.record(attempt=3), identity)  # replay after resolution
        new = self.record("fourth", time=self.time+timedelta(days=3))
        self.assertNotEqual(new, identity)
        self.assertEqual(len(list_alerts(status="ALL")), 2)
        self.assertEqual(len(list_alerts()), 1)

    def test_retries_do_not_increment_or_unacknowledge(self):
        identity = self.record()
        acknowledge_alert(identity, by="operator")
        for attempt in (1, 2, 2, 3):
            self.assertEqual(self.record(attempt=attempt), identity)
        self.assertEqual(self.fetch("SELECT status,occurrence_count,attempt_number FROM monitoring.alert_events"),
                         [("ACKNOWLEDGED",1,3)])
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.alert_occurrences"), [(1,)])

    def test_invalid_transitions_and_unknown_id_leave_audit_unchanged(self):
        identity = self.record()
        with self.assertRaises(AlertError):
            acknowledge_alert(identity, by=" ")
        with self.assertRaises(AlertError):
            resolve_alert(uuid4(), by="operator")
        resolve_alert(identity, by="operator")
        for operation in (acknowledge_alert, resolve_alert):
            with self.assertRaises(AlertError):
                operation(identity, by="operator")
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.alert_event_history"), [(2,)])

    def test_audit_failure_rolls_back_operator_state(self):
        identity = self.record()
        with psycopg.connect(**self.config) as connection:
            connection.execute("ALTER TABLE monitoring.alert_event_history ADD CONSTRAINT reject_test_actor "
                               "CHECK (changed_by <> 'reject')")
        try:
            with self.assertRaises(PersistenceError):
                acknowledge_alert(identity, by="reject")
            self.assertEqual(list_alerts()[0]["status"], "OPEN")
            self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.alert_event_history"), [(1,)])
        finally:
            with psycopg.connect(**self.config) as connection:
                connection.execute("ALTER TABLE monitoring.alert_event_history DROP CONSTRAINT reject_test_actor")

    def test_concurrent_duplicate_and_distinct_occurrences(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            identities = list(pool.map(self.record, ["same"]*4 + ["two","three","four"]))
        self.assertEqual(len(set(identities)), 1)
        self.assertEqual(list_alerts()[0]["occurrence_count"], 4)
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.alert_event_history"), [(1,)])

    def test_dimensions_are_separate_and_older_occurrences_keep_latest_evidence(self):
        self.record("new", time=self.time+timedelta(days=1), dimensions={"currency":"USD"}, severity="CRITICAL")
        self.record("old", dimensions={"currency":"USD"})
        self.record("eur", dimensions={"currency":"EUR"})
        row = list_alerts()[0]
        self.assertEqual((row["execution_id"],row["severity"],row["occurrence_count"]), ("new","CRITICAL",2))
        self.assertEqual(len(list_alerts()), 2)

    def test_anomaly_retry_normal_and_insufficient_preserve_manual_lifecycle(self):
        context = ExecutionContext(execution_id="evaluation")
        result = self.anomaly(context)
        persist_anomalies([result], context)
        identity = list_alerts()[0]["alert_event_id"]
        acknowledge_alert(identity, by="operator")
        persist_anomalies([result], replace(context, attempt_number=2))
        for status in (AnomalyStatus.NORMAL, AnomalyStatus.INSUFFICIENT_HISTORY):
            persist_anomalies([replace(result, status=status, severity=Severity.INFO)], context)
        self.assertEqual((list_alerts()[0]["status"],list_alerts()[0]["occurrence_count"]), ("ACKNOWLEDGED",1))
        persist_anomalies([], context)
        self.assertEqual(len(list_alerts()), 1)

    def test_quality_recurs_across_runs_and_retry_then_pass_does_not_resolve(self):
        result = sample_result(Status.FAIL, Severity.CRITICAL)
        run = fixture_run(result)
        context = ExecutionContext(execution_id="quality")
        persist_quality_run(run, context)
        identity = list_alerts()[0]["alert_event_id"]
        acknowledge_alert(identity, by="operator")
        persist_quality_run(run, replace(context, attempt_number=2))
        persist_quality_run(run, replace(context, execution_id="quality-next"))
        persist_quality_run(fixture_run(sample_result()), replace(context, execution_id="quality-pass"))
        self.assertEqual((list_alerts()[0]["status"],list_alerts()[0]["occurrence_count"]), ("ACKNOWLEDGED",2))
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.quality_runs"), [(4,)])

    def test_views_read_only_empty_and_dashboard_filter_semantics(self):
        for view in ("active_alerts","alert_history","alert_summary_by_status","alert_summary_by_severity","recurring_alerts"):
            self.assertEqual(self.fetch(f"SELECT count(*) FROM monitoring_views.{view}"), [(0,)])
            self.assertEqual(self.fetch("SELECT is_updatable,is_insertable_into FROM information_schema.views "
                                       "WHERE table_schema='monitoring_views' AND table_name=%s", (view,)), [("NO","NO")])
        first = self.record()
        self.record("again")
        acknowledge_alert(first, by="operator")
        second = self.record("other", dimensions={"currency":"EUR"})
        resolve_alert(second, by="operator")
        self.assertEqual(self.fetch("SELECT occurrence_count FROM monitoring_views.recurring_alerts"), [(2,)])
        for name in ("active_alerts","alerts_by_status","recurring_alerts","resolved_alerts"):
            query = question_query(name, 1)["native"]["query"]
            # Execute real card SQL with the lifecycle variable bound, omit other optional clauses.
            query = re.sub(r"\[\[(.*?)\]\]", lambda m: m[1] if "{{lifecycle_status}}" in m[1] else "", query, flags=re.S)
            query = query.replace("{{lifecycle_status}}", "%s")
            rows = self.fetch(query, ("ACKNOWLEDGED",))
            self.assertEqual(len(rows), 0 if name=="resolved_alerts" else 1)

    def test_repeatable_upgrade_of_legacy_duplicates_preserves_ids_and_retry_receipts(self):
        # Reset only our disposable schema and install the actual Phase 5.5 DDL prefix.
        source = Path("src/quality/monitoring.sql").read_text().split("-- Phase 5.6")[0]
        with psycopg.connect(**self.config) as connection:
            connection.execute("DROP SCHEMA monitoring CASCADE")
            connection.execute(source)
            ids = [uuid4(), uuid4()]
            for index, identity in enumerate(ids):
                connection.execute("""INSERT INTO monitoring.alert_events VALUES
                    (%s,'QUALITY_FAILURE',%s,'fixture','gold','CRITICAL','OPEN','Legacy','Failure',%s,
                     'cli',%s,NULL,NULL,NULL,1,-1,NULL,'{"check_name":"count","metric_name":"rows"}')""",
                    (identity,uuid4(),self.time+timedelta(days=index),str(index)))
        ensure_monitoring_schema()
        ensure_monitoring_schema()
        ensure_monitoring_views()
        self.assertEqual({r[0] for r in self.fetch("SELECT alert_event_id FROM monitoring.alert_events")}, set(ids))
        self.assertEqual((list_alerts()[0]["alert_event_id"],list_alerts()[0]["occurrence_count"]), (ids[0],2))
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.alert_occurrences"), [(2,)])
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.alert_event_history"), [(3,)])


if __name__ == "__main__":
    unittest.main()
