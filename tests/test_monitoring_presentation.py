"""Presentation contracts and opt-in SQL semantics against isolated PostgreSQL."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import re
import unittest
from unittest.mock import patch
from uuid import uuid4

import psycopg
from psycopg import sql

from bi import monitoring_dashboard as dashboard
from bi.setup_metabase import _unique
from src.quality.execution import ExecutionContext
from src.quality.models import Severity, Status
from src.quality.persistence import ensure_monitoring_schema, persist_quality_run
from src.warehouse.monitoring import ensure_monitoring_views
from src.warehouse.load_gold import connection_kwargs
from tests.test_quality import sample_result
from tests.test_quality_persistence import fixture_run


class DashboardContractsTests(unittest.TestCase):
    def test_health_inventory_matches_pipeline_policies(self):
        from src.quality.datasets import GOLD_GRAINS
        source = (Path(__file__).resolve().parents[1] / "src/warehouse/monitoring_views.sql").read_text()
        inventory = set(re.findall(r"\('([^']+)', '(silver|gold|analytics)'\)", source))
        self.assertEqual(inventory, {("silver_valid", "silver")} |
                         {(name, layer) for name in GOLD_GRAINS for layer in ("gold", "analytics")})

    def test_main_provisions_both_dashboards(self):
        from bi import setup_metabase as setup
        with patch.object(setup, "_request", return_value={}), patch.object(setup, "_login", return_value="session"), \
             patch.object(setup, "_ensure_warehouse", return_value=123), patch.object(setup, "_verify_marts"), \
             patch.object(setup, "_ensure_dashboard") as marketplace, \
             patch.object(dashboard, "ensure_dashboard", return_value=999) as health:
            self.assertEqual(setup.main(), 0)
            marketplace.assert_called_once_with("session", 123)
            self.assertEqual(health.call_args.args[2], 123)

    def test_queries_are_select_only_and_filters_match_semantics(self):
        self.assertEqual(len(dashboard.SPECS), 8)
        for filename, _, _, _ in dashboard.SPECS:
            query = dashboard.question_query(filename, 123)["native"]
            self.assertTrue(query["query"].startswith("SELECT "))
            self.assertNotRegex(query["query"].lower(), r"\b(insert|update|delete|truncate|drop|create|alter|call)\b")
            self.assertNotIn("monitoring.", query["query"])
            self.assertIn("monitoring_views.", query["query"])
            self.assertEqual(set(re.findall(r"\{\{(\w+)\}\}", query["query"])), set(query["template-tags"]))
            tags = set(query["template-tags"])
            if filename == "current_health":
                self.assertEqual(tags, {"layer"})
            elif filename == "latest_quality_status":
                self.assertEqual(tags, {"layer", "dataset"})
            elif filename in ("recent_warnings", "recent_critical_failures", "failing_checks"):
                self.assertEqual(tags, {"layer", "dataset", "start_date", "end_date"})
            else:
                self.assertEqual(tags, set(dashboard.FILTERS))

    def test_provisioning_is_idempotent_preserves_unrelated_cards_and_maps_valid_tags(self):
        objects = {}
        next_id = 70

        def api(method, path, payload=None):
            nonlocal next_id
            kind = path.split("/")[2]
            if path == "/api/dataset":
                return {"status": "completed", "data": {"rows": []}}
            if method == "GET" and path == "/api/collection":
                return [o.copy() for o in objects.values() if o["model"] == "collection"]
            if method == "GET" and path.endswith("/items"):
                collection_id = int(path.split("/")[3])
                return {"data": [o.copy() for o in objects.values() if o.get("collection_id") == collection_id]}
            if method == "POST":
                next_id += 1
                objects[next_id] = {**payload, "id": next_id, "model": kind}
                return objects[next_id].copy()
            identity = int(path.split("/")[3])
            if method == "PUT":
                objects[identity].update(payload)
            return objects[identity].copy()

        identity = dashboard.ensure_dashboard(api, _unique, 123)
        objects[identity]["dashcards"].append({"id": 900, "card_id": 901, "parameter_mappings": []})
        objects[identity]["parameters"].append({"id": "custom", "name": "Custom"})
        original_ids = set(objects)
        self.assertEqual(dashboard.ensure_dashboard(api, _unique, 123), identity)
        self.assertEqual(set(objects), original_ids)
        self.assertEqual(objects[identity]["name"], "Pulse Platform Health")
        self.assertEqual(len(objects[identity]["dashcards"]), 9)
        self.assertIn("custom", {p["id"] for p in objects[identity]["parameters"]})
        for card in objects[identity]["dashcards"][:-1]:
            tags = objects[card["card_id"]]["dataset_query"]["native"]["template-tags"]
            self.assertEqual({m["parameter_id"] for m in card["parameter_mappings"]}, set(tags))
            for mapping in card["parameter_mappings"]:
                self.assertEqual(mapping["target"], ["variable", ["template-tag", mapping["parameter_id"]]])

    def test_missing_views_fail_before_creating_metadata(self):
        calls = []
        def api(method, path, payload):
            calls.append(path)
            return {"status": "failed"}
        with self.assertRaisesRegex(RuntimeError, "src.warehouse.monitoring"):
            dashboard.ensure_dashboard(api, _unique, 123)
        self.assertEqual(calls, ["/api/dataset"])

    def test_view_initializer_rolls_back_and_sanitizes_errors(self):
        with patch("src.warehouse.monitoring.psycopg.connect") as connect:
            connection = connect.return_value.__enter__.return_value
            connection.execute.side_effect = [None, None, Exception("password=private")]
            with self.assertRaisesRegex(RuntimeError, "initialize the Phase 5.3 tables") as error:
                ensure_monitoring_views()
            self.assertNotIn("private", str(error.exception))
            self.assertIsNotNone(connect.return_value.__exit__.call_args.args[0])


@unittest.skipUnless(os.environ.get("RUN_MONITORING_INTEGRATION_TESTS") == "1",
                     "Requires disposable PostgreSQL database; set RUN_MONITORING_INTEGRATION_TESTS=1")
class PresentationPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = connection_kwargs()
        cls.database = "pulse_views_test_" + uuid4().hex
        with psycopg.connect(**cls.base, autocommit=True) as c:
            c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cls.database)))
        cls.addClassCleanup(cls.cleanup_database)
        cls.config = {**cls.base, "dbname": cls.database}
        for module in ("src.quality.persistence", "src.warehouse.monitoring"):
            override = patch(module + ".connection_kwargs", return_value=cls.config)
            override.start()
            cls.addClassCleanup(override.stop)
        ensure_monitoring_schema()
        ensure_monitoring_views()

    @classmethod
    def cleanup_database(cls):
        with psycopg.connect(**cls.base, autocommit=True) as c:
            c.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(cls.database)))

    def setUp(self):
        with psycopg.connect(**self.config) as c:
            c.execute("TRUNCATE monitoring.quality_results, monitoring.quality_runs")

    def fetch(self, query, params=()):
        with psycopg.connect(**self.config) as c:
            return c.execute(query, params).fetchall()

    def insert(self, status=Status.PASS, severity=Severity.CRITICAL, dataset="silver_valid", layer="silver", day=1):
        timestamp = datetime(2026, 1, day, tzinfo=timezone.utc)
        result = replace(sample_result(status, severity), checked_at_utc=timestamp,
                         dataset_name=dataset, layer=layer)
        run = replace(fixture_run(), dataset_name=dataset, layer=layer, results=(result,),
                      started_at_utc=timestamp - timedelta(seconds=2), completed_at_utc=timestamp)
        return persist_quality_run(run, ExecutionContext(execution_id=str(uuid4())))

    def test_empty_history_is_unknown_and_views_are_repeatable_read_only(self):
        ensure_monitoring_views()
        for name in ("quality_history", "latest_quality_status", "check_history", "check_failure_summary", "recent_critical_failures"):
            self.assertEqual(self.fetch(f"SELECT count(*) FROM monitoring_views.{name}"), [(0,)])
        self.assertEqual(self.fetch("SELECT layer,overall_status,observed_datasets FROM monitoring_views.current_health ORDER BY layer"),
                         [("analytics", "UNKNOWN", 0), ("gold", "UNKNOWN", 0), ("silver", "UNKNOWN", 0)])
        for name in ("quality_history", "latest_quality_status", "check_history", "check_failure_summary", "recent_critical_failures", "current_health"):
            with self.subTest(view=name), psycopg.connect(**self.config) as c:
                with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
                    c.execute(f"DELETE FROM monitoring_views.{name}")

    def test_actual_latest_completion_and_dataset_layer_isolation(self):
        self.insert(Status.PASS, day=3)
        self.insert(Status.FAIL, day=1)  # Inserted later, completed earlier.
        self.insert(Status.WARN, Severity.WARNING, dataset="other", day=4)
        self.insert(Status.FAIL, dataset="silver_valid", layer="gold", day=4)
        rows = self.fetch("SELECT dataset_name,layer,overall_status FROM monitoring_views.latest_quality_status ORDER BY 1,2")
        self.assertEqual(rows, [("other", "silver", "WARN"), ("silver_valid", "gold", "FAIL"), ("silver_valid", "silver", "PASS")])
        self.assertEqual(self.fetch("SELECT overall_status FROM monitoring_views.current_health WHERE layer='silver'"), [("PASS",)])
        self.assertEqual(self.fetch("SELECT duration_seconds FROM monitoring_views.quality_history"), [(2,)] * 4)

    def test_tied_completion_is_deterministic(self):
        ids = [self.insert(Status.FAIL), self.insert(Status.PASS)]
        self.assertEqual(self.fetch("SELECT quality_run_id FROM monitoring_views.latest_quality_status"), [(max(ids),)])

    def test_severity_status_counts_and_time_windows(self):
        self.insert(Status.PASS, Severity.WARNING)
        self.insert(Status.PASS, Severity.CRITICAL)
        self.insert(Status.WARN, Severity.CRITICAL, day=2)
        self.insert(Status.WARN, Severity.WARNING, day=2)
        self.insert(Status.FAIL, Severity.CRITICAL, day=3)
        self.assertEqual(self.fetch("SELECT failure_count,warning_count,critical_failure_count FROM monitoring_views.check_failure_summary"), [(1, 2, 1)])
        self.assertEqual(self.fetch("SELECT status,severity,observed_value FROM monitoring_views.recent_critical_failures"), [("FAIL", "CRITICAL", 1)])
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring_views.check_history WHERE status='WARN' AND checked_date_utc=%s", ("2026-01-02",)), [(2,)])
        self.assertEqual(self.fetch("SELECT overall_status,should_block FROM monitoring_views.current_health WHERE layer='silver'"), [("FAIL", True)])
        self.assertEqual(self.fetch("SELECT latest_successful_check_at_utc FROM monitoring_views.current_health WHERE layer='silver'")[0][0].day, 1)

    def test_coverage_prevents_partial_gold_pass_and_failure_remains_visible(self):
        self.insert(dataset="daily_sales", layer="gold")
        self.assertEqual(self.fetch("SELECT overall_status,observed_datasets,expected_datasets FROM monitoring_views.current_health WHERE layer='gold'"), [("UNKNOWN", 1, 4)])
        self.insert(Status.FAIL, dataset="customer_metrics", layer="gold")
        self.assertEqual(self.fetch("SELECT overall_status FROM monitoring_views.current_health WHERE layer='gold'"), [("FAIL",)])
        for name in ("customer_metrics", "product_metrics", "funnel_metrics"):
            self.insert(dataset=name, layer="gold", day=2)
        self.assertEqual(self.fetch("SELECT overall_status,observed_datasets FROM monitoring_views.current_health WHERE layer='gold'"), [("PASS", 4)])

    def test_all_dashboard_questions_execute_with_empty_and_populated_history(self):
        for populated in (False, True):
            if populated:
                self.insert()
            for name, _, _, _ in dashboard.SPECS:
                query = dashboard.question_query(name, 0)["native"]["query"]
                unfiltered = re.sub(r"\[\[.*?\]\]", "", query)
                self.fetch(unfiltered)
                filtered = query.replace("[[", "").replace("]]", "")
                values = {"layer": "silver", "dataset": "silver_valid", "status": "PASS",
                          "start_date": "2026-01-01", "end_date": "2026-01-01"}
                filtered = re.sub(r"\{\{(\w+)\}\}", lambda m: f"%({m[1]})s", filtered)
                self.fetch(filtered, values)
