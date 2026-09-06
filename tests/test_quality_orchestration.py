"""Quality boundary selection, CLI exit policy, logs, and warehouse snapshots."""

from contextlib import contextmanager
from datetime import date
import json
import os
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

import psycopg

from src.analytics.gold_build import build_gold_spark_session, SILVER_VALID_SCHEMA
from src.quality.datasets import GOLD_GRAINS, gold_rules, silver_rules
from src.quality.models import Severity, Status, should_block
from src.quality.runner import iter_target_results, main
from src.quality.warehouse import warehouse_frames
from src.warehouse.load_gold import TABLE_SPECS
from tests.test_quality import sample_result


class QualityBoundaryTests(unittest.TestCase):
    def test_silver_alias_selects_existing_silver_policy_and_configured_path(self):
        with patch("src.utils.parquet.read_parquet_data_files") as read, \
             patch("src.streaming.silver_streaming.load_silver_paths") as paths, \
             patch("src.quality.runner.run_quality_checks", return_value=[]) as run:
            paths.return_value.valid = Path("configured-silver")
            list(iter_target_results(MagicMock(), "silver"))
        self.assertEqual(read.call_args.args[1], Path("configured-silver").resolve())
        self.assertEqual(run.call_args.args[1], silver_rules())
        self.assertEqual(run.call_args.args[2].dataset_name, "silver_valid")
        self.assertEqual(run.call_args.args[2].layer, "silver")

    def test_gold_selects_each_existing_gold_policy(self):
        with patch("src.utils.parquet.read_parquet_data_files"), \
             patch("src.quality.runner.run_quality_checks", return_value=[]) as run:
            list(iter_target_results(MagicMock(), "gold"))
        self.assertEqual(run.call_count, 4)
        for call, name in zip(run.call_args_list, GOLD_GRAINS):
            self.assertEqual(call.args[1], gold_rules(name))
            self.assertEqual((call.args[2].dataset_name, call.args[2].layer), (name, "gold"))

    def test_warehouse_uses_actual_snapshot_with_analytics_policy_and_closes_it(self):
        closed = []
        frames = {name: object() for name in GOLD_GRAINS}

        @contextmanager
        def snapshot(_spark):
            try:
                yield frames
            finally:
                closed.append(True)

        with patch("src.quality.warehouse.warehouse_frames", snapshot), \
             patch("src.quality.runner.run_quality_checks", return_value=[]) as run:
            list(iter_target_results(MagicMock(), "warehouse"))
        self.assertEqual(closed, [True])
        self.assertEqual(run.call_count, 4)
        for call, name in zip(run.call_args_list, GOLD_GRAINS):
            self.assertIs(call.args[0], frames[name])
            self.assertEqual(call.args[1], gold_rules(name, layer="analytics"))
            self.assertEqual(call.args[2].layer, "analytics")

    def test_info_warning_continue_and_critical_exits_one_with_full_log(self):
        for severity, status, expected in ((Severity.INFO, Status.WARN, 0),
                                            (Severity.WARNING, Status.WARN, 0),
                                            (Severity.CRITICAL, Status.FAIL, 1)):
            result = sample_result(status, severity)
            with self.subTest(severity=severity), \
                 patch("src.analytics.gold_build.build_gold_spark_session") as spark, \
                 patch("src.quality.runner.iter_target_results", return_value=iter([sample_result(), result])), \
                 patch("builtins.print") as output:
                self.assertEqual(main(["silver", "--block-on-critical", "--log-format", "jsonl"]), expected)
                events = [json.loads(call.args[0]) for call in output.call_args_list]
                self.assertTrue(all(call.kwargs["flush"] for call in output.call_args_list))
            spark.return_value.stop.assert_called_once()
            self.assertEqual([e["event"] for e in events], ["quality_result", "quality_result", "quality_summary"])
            logged = events[1]
            for field in ("dataset_name", "layer", "check_name", "severity", "status",
                          "observed_value", "expected_value", "checked_at_utc"):
                self.assertIn(field, logged)
            self.assertEqual(logged["checked_at_utc"], result.checked_at_utc.isoformat())
            self.assertEqual(events[-1]["counts"], {"PASS": 1, "WARN": int(expected == 0), "FAIL": expected})
            self.assertTrue(events[-1]["completed"])

    def test_report_only_default_and_json_shape_remain_unchanged(self):
        with patch("src.analytics.gold_build.build_gold_spark_session"), \
             patch("src.quality.runner.iter_target_results", return_value=[sample_result(Status.FAIL)]), \
             patch("builtins.print") as output:
            self.assertEqual(main(["daily_sales"]), 0)
        report = json.loads(output.call_args.args[0])
        self.assertEqual(set(report), {"results", "summary"})
        self.assertEqual(report["summary"]["critical_failures"], 1)

    def test_operational_error_logs_partial_results_and_raises(self):
        def failing(*_args, **_kwargs):
            yield sample_result()
            raise OSError("unreadable next dataset")

        with patch("src.analytics.gold_build.build_gold_spark_session") as spark, \
             patch("src.quality.runner.iter_target_results", failing), \
             patch("builtins.print") as output, self.assertRaises(OSError):
            main(["gold", "--block-on-critical", "--log-format", "jsonl"])
        events = [json.loads(call.args[0]) for call in output.call_args_list]
        self.assertEqual(events[-2]["event"], "quality_execution_error")
        self.assertFalse(events[-1]["completed"])
        self.assertEqual(events[-1]["total_checks"], 1)
        spark.return_value.stop.assert_called_once()

    def test_group_targets_reject_ambiguous_options_before_starting_spark(self):
        for args in (["gold", "--path", "x"], ["warehouse", "--reference-count", "2"],
                     ["gold", "--max-age-hours", "24"], ["silver", "--reference-count", "-1"]):
            with self.subTest(args=args), patch("sys.stderr"), \
                 patch("src.analytics.gold_build.build_gold_spark_session") as spark, \
                 self.assertRaises(SystemExit) as raised:
                main(args)
            self.assertEqual(raised.exception.code, 2)
            spark.assert_not_called()


def mock_warehouse(rows_by_name=None):
    """Mock only database I/O; snapshot serialization and Spark remain real."""
    connection = MagicMock()
    catalog = MagicMock()
    catalog.fetchall.side_effect = [
        [(col.name, col.postgres_type.lower()) for col in spec.columns] for spec in TABLE_SPECS
    ]

    def cursor(*, name=None):
        result = MagicMock()
        if name is None:
            result.__enter__.return_value = catalog
        else:
            data = (rows_by_name or {}).get(name.removeprefix("quality_"), [])
            result.__enter__.return_value.__iter__.side_effect = lambda: iter(
                [(json.dumps(row),) for row in data]
            )
        return result

    connection.cursor.side_effect = cursor
    return connection, catalog


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS", "1") == "1", "Spark tests disabled")
class QualityBoundarySparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = build_gold_spark_session(app_name="pulse-quality-boundaries-tests", master="local[1]")
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.conf.set("spark.sql.shuffle.partitions", "2")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_empty_silver_warning_does_not_block_runner(self):
        frame = self.spark.createDataFrame([], SILVER_VALID_SCHEMA)
        with patch("src.utils.parquet.read_parquet_data_files", return_value=frame):
            results = list(iter_target_results(self.spark, "silver"))
        self.assertFalse(should_block(results))
        self.assertEqual(results[0].status, Status.WARN)

    def test_warehouse_snapshot_preserves_types_nulls_empty_tables_and_enforces_rules(self):
        row = dict(event_date="2026-01-01", country="US", currency="USD",
                   completed_orders=1, units_sold=2, gross_revenue=-3.0, avg_order_value=None)
        connection, _ = mock_warehouse({"daily_sales": [row, row]})
        with patch("src.quality.warehouse.psycopg.connect") as connect:
            connect.return_value.__enter__.return_value = connection
            with warehouse_frames(self.spark) as frames:
                record = frames["daily_sales"].first()
                self.assertEqual(record.event_date, date(2026, 1, 1))
                self.assertIsNone(record.avg_order_value)
                self.assertEqual(frames["customer_metrics"].count(), 0)
            # Recreate the database cursor responses for a full boundary evaluation.
            connection, _ = mock_warehouse({"daily_sales": [row, row]})
            connect.return_value.__enter__.return_value = connection
            results = list(iter_target_results(self.spark, "warehouse"))
        failures = {(r.dataset_name, r.check_name) for r in results if r.status == Status.FAIL}
        self.assertIn(("daily_sales", "grain_unique"), failures)
        self.assertIn(("daily_sales", "gross_revenue_nonnegative"), failures)
        for name in ("customer_metrics", "product_metrics", "funnel_metrics"):
            self.assertIn((name, "row_count"), failures)
        self.assertTrue(connection.read_only)
        self.assertEqual(connection.isolation_level, psycopg.IsolationLevel.REPEATABLE_READ)

    def test_warehouse_missing_columns_or_wrong_types_fail_before_quality(self):
        for columns in ([], [(col.name, "text") for col in TABLE_SPECS[0].columns]):
            connection, catalog = mock_warehouse()
            catalog.fetchall.side_effect = [columns]
            with patch("src.quality.warehouse.psycopg.connect") as connect:
                connect.return_value.__enter__.return_value = connection
                with self.assertRaises(ValueError):
                    list(iter_target_results(self.spark, "warehouse"))


@unittest.skipUnless(os.environ.get("RUN_WAREHOUSE_INTEGRATION_TESTS") == "1",
                     "Requires populated local warehouse; set RUN_WAREHOUSE_INTEGRATION_TESTS=1")
class WarehouseQualityIntegrationTests(unittest.TestCase):
    def test_live_warehouse_passes_all_four_analytics_policies(self):
        spark = build_gold_spark_session(app_name="pulse-warehouse-quality-test", master="local[1]")
        spark.sparkContext.setLogLevel("ERROR")
        try:
            results = list(iter_target_results(spark, "warehouse"))
            self.assertEqual({r.dataset_name for r in results}, set(GOLD_GRAINS))
            self.assertTrue(all(r.layer == "analytics" for r in results))
            self.assertFalse(should_block(results), [r for r in results if r.status == Status.FAIL])
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
