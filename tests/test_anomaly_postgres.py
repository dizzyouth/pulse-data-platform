"""Opt-in isolated PostgreSQL tests for anomaly and alert persistence."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import os
import unittest
from unittest.mock import patch
from uuid import uuid4

import psycopg
from psycopg import sql

from src.quality.anomaly import AnomalyResult, AnomalyStatus
from src.quality.anomaly_persistence import persist_anomalies
from src.quality.anomaly_sources import load_metric_series
from src.quality.execution import ExecutionContext
from src.quality.models import Severity, Status
from src.quality.persistence import PersistenceError, ensure_monitoring_schema, persist_quality_run
from src.warehouse.load_gold import connection_kwargs
from src.warehouse.monitoring import ensure_monitoring_views
from tests.test_quality import sample_result
from tests.test_quality_persistence import fixture_run


@unittest.skipUnless(os.environ.get("RUN_MONITORING_INTEGRATION_TESTS") == "1",
                     "Requires disposable PostgreSQL database; set RUN_MONITORING_INTEGRATION_TESTS=1")
class AnomalyPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = connection_kwargs()
        cls.database = "pulse_anomaly_test_" + uuid4().hex
        with psycopg.connect(**cls.base, autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cls.database)))
        cls.config = {**cls.base, "dbname": cls.database}
        cls.addClassCleanup(cls.cleanup_database)
        for module in ("src.quality.persistence", "src.quality.anomaly_persistence",
                       "src.quality.anomaly_sources", "src.warehouse.monitoring"):
            override = patch(module + ".connection_kwargs", return_value=cls.config)
            override.start()
            cls.addClassCleanup(override.stop)
        ensure_monitoring_schema()
        ensure_monitoring_views()
        with psycopg.connect(**cls.config) as connection:
            connection.execute("CREATE SCHEMA analytics")
            connection.execute("""CREATE TABLE analytics.daily_sales (
                event_date date,currency text,completed_orders bigint,gross_revenue double precision)""")
            connection.execute("""CREATE TABLE analytics.funnel_metrics (
                event_date date,country text,view_to_cart_rate double precision,
                cart_to_checkout_rate double precision,checkout_to_order_rate double precision,
                order_to_payment_rate double precision)""")

    @classmethod
    def cleanup_database(cls):
        with psycopg.connect(**cls.base, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(cls.database)))

    def setUp(self):
        with psycopg.connect(**self.config) as connection:
            connection.execute("TRUNCATE monitoring.alert_events,monitoring.anomaly_results,"
                               "monitoring.quality_results,monitoring.quality_runs,"
                               "analytics.daily_sales,analytics.funnel_metrics")

    def fetch(self, query, params=()):
        with psycopg.connect(**self.config) as connection:
            return connection.execute(query, params).fetchall()

    def context(self, attempt=1):
        return ExecutionContext(execution_source="airflow", execution_id="run", dag_id="pulse",
                                airflow_run_id="run", task_id="anomaly_check",
                                attempt_number=attempt, logical_date_utc=datetime(2026, 1, 8, tzinfo=timezone.utc))

    def anomaly(self, context, status=AnomalyStatus.ANOMALY, severity=Severity.WARNING):
        return AnomalyResult(anomaly_id=context.logical_id("pulse-anomaly-result-v1", "daily_sales",
            "analytics", "gross_revenue", '{"currency":"USD"}'), metric_name="gross_revenue",
            dataset_name="daily_sales", layer="analytics", dimensions={"currency": "USD"},
            current_value=200, baseline_value=100, deviation_value=100, deviation_percent=100,
            threshold={"warning_ratio": .5}, method="percentage_deviation", status=status,
            severity=severity, observed_at_utc=datetime(2026, 1, 8, tzinfo=timezone.utc),
            explanation="Current 200; median baseline 100; deviation 100; threshold 0.5.",
            history_count=7, details={"score": 1})

    def test_anomaly_and_alert_persist_atomically_and_retry_replaces(self):
        first = self.context()
        evaluation_id = persist_anomalies([self.anomaly(first)], first)
        retry = self.context(2)
        self.assertEqual(persist_anomalies([self.anomaly(retry)], retry), evaluation_id)
        self.assertEqual(self.fetch("SELECT count(*),max(attempt_number) FROM monitoring.anomaly_results"), [(1, 2)])
        self.assertEqual(self.fetch("SELECT source_type,severity,status,count(*) FROM monitoring.alert_events GROUP BY 1,2,3"),
                         [("ANOMALY", "WARNING", "OPEN", 1)])
        row = self.fetch("SELECT current_value,baseline_value,dimensions,method,history_count FROM monitoring.anomaly_results")[0]
        self.assertEqual(row, (200, 100, {"currency": "USD"}, "percentage_deviation", 7))

    def test_normal_and_insufficient_results_do_not_alert(self):
        context = self.context()
        results = [replace(self.anomaly(context), anomaly_id=uuid4(), status=AnomalyStatus.NORMAL,
                           severity=Severity.INFO),
                   replace(self.anomaly(context), anomaly_id=uuid4(), status=AnomalyStatus.INSUFFICIENT_HISTORY,
                           severity=Severity.INFO, baseline_value=None, deviation_value=None,
                           deviation_percent=None, history_count=1)]
        persist_anomalies(results, context)
        self.assertEqual(self.fetch("SELECT status,count(*) FROM monitoring.anomaly_results GROUP BY 1 ORDER BY 1"),
                         [("INSUFFICIENT_HISTORY", 1), ("NORMAL", 1)])
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.alert_events"), [(0,)])

    def test_critical_quality_failure_alert_is_deduplicated_across_retry(self):
        context = replace(self.context(), task_id="quality_check_warehouse")
        result = replace(sample_result(Status.FAIL, Severity.CRITICAL), dataset_name="fixture", layer="analytics")
        run = replace(fixture_run(), dataset_name="fixture", layer="analytics", results=(result,))
        persist_quality_run(run, context)
        persist_quality_run(run, replace(context, attempt_number=2))
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.quality_runs"), [(2,)])
        self.assertEqual(self.fetch("SELECT source_type,severity,status,attempt_number,count(*) "
                                    "FROM monitoring.alert_events GROUP BY 1,2,3,4"),
                         [("QUALITY_FAILURE", "CRITICAL", "OPEN", 2, 1)])

    def test_quality_alert_failure_rolls_back_run_and_result(self):
        with psycopg.connect(**self.config) as connection:
            connection.execute("ALTER TABLE monitoring.alert_events ADD CONSTRAINT reject_quality_alert "
                               "CHECK (source_type <> 'QUALITY_FAILURE')")
        try:
            context = replace(self.context(), task_id="quality_check_warehouse")
            result = replace(sample_result(Status.FAIL, Severity.CRITICAL), dataset_name="fixture", layer="analytics")
            run = replace(fixture_run(), dataset_name="fixture", layer="analytics", results=(result,))
            with self.assertRaises(PersistenceError):
                persist_quality_run(run, context)
            self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.quality_runs"), [(0,)])
            self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.quality_results"), [(0,)])
        finally:
            with psycopg.connect(**self.config) as connection:
                connection.execute("ALTER TABLE monitoring.alert_events DROP CONSTRAINT reject_quality_alert")

    def test_sources_use_latest_retry_once_and_preserve_currency_country_series(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        for index in range(8):
            context = ExecutionContext(execution_source="airflow", execution_id=f"run-{index}", dag_id="pulse",
                                       airflow_run_id=f"run-{index}", task_id="quality_check_warehouse")
            result = replace(sample_result(), dataset_name="daily_sales", layer="analytics",
                             metric_name="row_count", observed_value=index + 10,
                             checked_at_utc=base + timedelta(days=index))
            run = replace(fixture_run(), dataset_name="daily_sales", layer="analytics", results=(result,),
                          started_at_utc=base + timedelta(days=index), completed_at_utc=base + timedelta(days=index))
            persist_quality_run(run, context)
            if index == 7:
                persist_quality_run(run, replace(context, attempt_number=2))
        with psycopg.connect(**self.config) as connection:
            for index in range(8):
                day = (base + timedelta(days=index)).date()
                connection.execute("INSERT INTO analytics.daily_sales VALUES (%s,'USD',%s,%s),(%s,'EUR',%s,%s)",
                                   (day, index + 1, 100 + index, day, index + 2, 200 + index))
                connection.execute("INSERT INTO analytics.funnel_metrics VALUES (%s,'US',%s,.5,.5,.5)",
                                   (day, .5 + index / 100))
        series = load_metric_series()
        indexed = {(item.metric_name, tuple(sorted(item.dimensions.items()))): item for item in series}
        self.assertEqual(len(indexed[("row_count", ())].history), 7)
        self.assertEqual(indexed[("completed_order_volume", ())].current_value, 17)
        self.assertEqual(indexed[("gross_revenue", (("currency", "USD"),))].current_value, 107)
        self.assertEqual(indexed[("gross_revenue", (("currency", "EUR"),))].current_value, 207)
        self.assertEqual(len(indexed[("view_to_cart_rate", (("country", "US"),))].history), 7)

    def test_new_presentation_views_are_read_only_and_semantically_exact(self):
        context = self.context()
        persist_anomalies([self.anomaly(context, severity=Severity.CRITICAL)], context)
        views = {row[0]: row[1:] for row in self.fetch("SELECT table_name,is_updatable,is_insertable_into "
                 "FROM information_schema.views WHERE table_schema='monitoring_views'")}
        for name in ("recent_anomalies", "recent_alert_events", "anomaly_summary_by_metric",
                     "alert_summary_by_severity"):
            self.assertEqual(views[name], ("NO", "NO"))
        self.assertEqual(self.fetch("SELECT status,severity FROM monitoring_views.recent_anomalies"),
                         [("ANOMALY", "CRITICAL")])
        self.assertEqual(self.fetch("SELECT anomaly_count,critical_anomaly_count FROM "
                                    "monitoring_views.anomaly_summary_by_metric"), [(1, 1)])
        self.assertEqual(self.fetch("SELECT source_type,severity,status,alert_count FROM "
                                    "monitoring_views.alert_summary_by_severity"),
                         [("ANOMALY", "CRITICAL", "OPEN", 1)])


if __name__ == "__main__":
    unittest.main()
