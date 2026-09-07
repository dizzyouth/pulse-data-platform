"""Deterministic anomaly engine, policies, CLI, and DAG contracts."""

from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from uuid import uuid4

from src.quality.anomaly import AnomalyPolicy, AnomalyStatus, MetricSeries, evaluate
from src.quality.anomaly_runner import evaluate_all, main, policy_for
from src.quality.execution import ExecutionContext
from src.quality.models import Severity


NOW = datetime(2026, 1, 8, tzinfo=timezone.utc)


def series(current, history=(10, 10, 10, 10, 10, 10, 10), metric="row_count"):
    return MetricSeries(metric_name=metric, dataset_name="daily_sales", layer="analytics",
                        current_value=current, history=history, observed_at_utc=NOW)


class AnomalyEngineTests(unittest.TestCase):
    def assess(self, current, history=(10, 10, 10, 10, 10, 10, 10), policy=None):
        return evaluate(series(current, history), policy or AnomalyPolicy(), uuid4())

    def test_stable_history_is_normal(self):
        result = self.assess(11, (8, 9, 10, 10, 10, 11, 12))
        self.assertEqual((result.status, result.severity, result.method),
                         (AnomalyStatus.NORMAL, Severity.INFO, "modified_z_score"))
        self.assertIn("median baseline 10", result.explanation)

    def test_sudden_drop_and_spike_are_explainable_critical_anomalies(self):
        for current in (0, 20):
            with self.subTest(current=current):
                result = self.assess(current)
                self.assertEqual((result.status, result.severity, result.method),
                                 (AnomalyStatus.ANOMALY, Severity.CRITICAL, "percentage_deviation"))
                self.assertEqual(result.baseline_value, 10)
                self.assertEqual(result.deviation_value, current - 10)
                self.assertIn("critical threshold", result.explanation)

    def test_insufficient_history_is_neither_normal_nor_failure(self):
        result = self.assess(100, (10,) * 6)
        self.assertEqual((result.status, result.severity, result.baseline_value),
                         (AnomalyStatus.INSUFFICIENT_HISTORY, Severity.INFO, None))
        self.assertIn("Need 7", result.explanation)

    def test_median_mad_resists_outlier_and_flags_new_spike(self):
        result = self.assess(30, (8, 9, 10, 10, 10, 11, 12))
        self.assertEqual(result.method, "modified_z_score")
        self.assertEqual(result.details["median_absolute_deviation"], 1)
        self.assertEqual(result.status, AnomalyStatus.ANOMALY)

    def test_threshold_boundary_is_inclusive_and_maps_severity(self):
        warning = self.assess(15)
        critical = self.assess(20)
        self.assertEqual((warning.status, warning.severity), (AnomalyStatus.ANOMALY, Severity.WARNING))
        self.assertEqual(critical.severity, Severity.CRITICAL)
        self.assertEqual(warning.threshold["boundary"], "inclusive")

    def test_zero_baseline_uses_absolute_count_threshold(self):
        policy = policy_for("warning_check_count", 7)
        self.assertEqual(self.assess(1, (0,) * 7, policy).severity, Severity.WARNING)
        self.assertEqual(self.assess(3, (0,) * 7, policy).severity, Severity.CRITICAL)

    def test_id_and_dimensions_are_stable_across_airflow_retries(self):
        context = ExecutionContext(execution_source="airflow", execution_id="run", dag_id="dag",
                                   airflow_run_id="run", task_id="anomaly_check", attempt_number=1)
        item = replace(series(10), dimensions={"currency": "USD"})
        first = evaluate_all([item], context, 7)[0]
        second = evaluate_all([item], replace(context, attempt_number=2), 7)[0]
        self.assertEqual(first.anomaly_id, second.anomaly_id)

    def test_warning_and_critical_default_nonblocking_and_opt_in_critical_blocks(self):
        warning, critical = series(15), series(20)
        def run(item, extra=()):
            with patch("src.quality.anomaly_sources.load_metric_series", return_value=[item]), \
                 patch("src.quality.anomaly_persistence.persist_anomalies", return_value=uuid4()), \
                 patch("builtins.print"):
                return main(["--persist", "--execution-id", "test", *extra])
        self.assertEqual(run(warning), 0)
        self.assertEqual(run(critical), 0)
        self.assertEqual(run(critical, ("--block-on-critical",)), 1)

    def test_structured_logging_follows_persistence(self):
        events = []
        def output(value, **_):
            events.append(json.loads(value)["event"])
        with patch("src.quality.anomaly_sources.load_metric_series", return_value=[series(15)]), \
             patch("src.quality.anomaly_persistence.persist_anomalies", return_value=uuid4()), \
             patch("builtins.print", side_effect=output):
            self.assertEqual(main(["--persist", "--execution-id", "test"]), 0)
        self.assertEqual(events, ["anomaly_persisted", "anomaly_result", "anomaly_summary"])


class AnomalyDagTests(unittest.TestCase):
    def test_single_nonblocking_task_sits_between_warehouse_quality_and_dbt(self):
        source = Path("airflow/dags/pulse_analytics_pipeline.py").read_text(encoding="utf-8")
        self.assertEqual(source.count('task_id="anomaly_check"'), 1)
        self.assertIn("quality_check_warehouse\n        >> anomaly_check\n        >> run_dbt", source)
        command = next(line for line in source.splitlines() if "anomaly_runner" in line)
        self.assertIn("--persist", command)
        self.assertNotIn("--block-on-critical", command)


if __name__ == "__main__":
    unittest.main()
