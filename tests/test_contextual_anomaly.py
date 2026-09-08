"""Contextual baseline, guardrail, backtest, and explain-CLI contracts."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import unittest
from unittest.mock import patch
from uuid import uuid4

from src.quality.anomaly import (AnomalyPolicy, AnomalyStatus, BaselineConfidence,
                                 MetricSeries, evaluate)
from src.quality.anomaly_backtest import backtest_series
from src.quality.anomaly_cli import main as explain_main
from src.quality.anomaly_runner import policy_for


START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def timed_series(values, metric="gross_revenue", samples=()):
    timestamps = tuple(START + timedelta(days=index) for index in range(len(values)))
    return MetricSeries(metric_name=metric, dataset_name="daily_sales", layer="analytics",
                        history=tuple(values[:-1]), current_value=values[-1],
                        history_observed_at_utc=timestamps[:-1], observed_at_utc=timestamps[-1],
                        current_sample_size=samples[-1] if samples else None,
                        history_sample_sizes=tuple(samples[:-1]) if samples else ())


def assess(item, policy):
    return evaluate(item, policy, uuid4())


class ContextualBaselineTests(unittest.TestCase):
    def test_weekday_baseline_uses_only_matching_weekdays(self):
        values = [100 + (index % 7) * 20 for index in range(29)]
        result = assess(timed_series(values, "completed_order_volume"),
                        AnomalyPolicy(baseline_strategies=("day_of_week", "robust_history")))
        self.assertEqual((result.baseline_strategy, result.seasonal_reference_count), ("day_of_week", 4))
        self.assertEqual(result.expected_value, values[-1])
        self.assertEqual(result.status, AnomalyStatus.NORMAL)

    def test_trend_baseline_forecasts_growth_without_false_anomaly(self):
        values = [10 + index * 2 for index in range(15)]
        result = assess(timed_series(values), AnomalyPolicy(baseline_strategies=("trend", "robust_history")))
        self.assertAlmostEqual(result.expected_value, values[-1])
        self.assertAlmostEqual(result.trend_slope, 2)
        self.assertEqual(result.status, AnomalyStatus.NORMAL)

    def test_seasonal_trend_combines_weekday_level_and_robust_slope(self):
        pattern = (0, 20, -10, 15, 30, -25, -30)
        values = [100 + 2 * index + pattern[index % 7] for index in range(43)]
        result = assess(timed_series(values),
                        AnomalyPolicy(baseline_strategies=("seasonal_trend", "day_of_week",
                                                           "trend", "robust_history")))
        self.assertEqual(result.baseline_strategy, "seasonal_trend")
        self.assertAlmostEqual(result.expected_value, values[-1])
        self.assertAlmostEqual(result.trend_slope, 2)
        self.assertEqual(result.status, AnomalyStatus.NORMAL)

    def test_selection_hierarchy_records_fallback(self):
        values = [100 + index for index in range(15)]
        result = assess(timed_series(values), policy_for("gross_revenue", 7))
        self.assertEqual(result.baseline_strategy, "trend")
        self.assertTrue(result.fallback_used)
        self.assertEqual(result.details["attempted_baseline_strategies"],
                         ["seasonal_trend", "day_of_week", "trend"])

    def test_contextual_policy_can_exhaust_to_insufficient_history(self):
        result = assess(timed_series([1, 2, 3]),
                        AnomalyPolicy(baseline_strategies=("day_of_week", "trend", "robust_history")))
        self.assertEqual(result.status, AnomalyStatus.INSUFFICIENT_HISTORY)
        self.assertIn("No configured baseline qualified", result.explanation)

    def test_zero_variance_and_outlier_resistance_are_safe(self):
        flat = assess(timed_series([10] * 8), AnomalyPolicy())
        self.assertEqual((flat.method, flat.status), ("percentage_deviation", AnomalyStatus.NORMAL))
        values = [10 + 2 * index for index in range(20)]
        values[8] += 500
        values.append(50)
        trend = assess(timed_series(values), AnomalyPolicy(baseline_strategies=("trend",)))
        self.assertAlmostEqual(trend.trend_slope, 2)
        self.assertEqual(trend.status, AnomalyStatus.NORMAL)

    def test_spike_drop_and_minimum_absolute_guardrail(self):
        policy = AnomalyPolicy(baseline_strategies=("day_of_week", "robust_history"),
                               warning_ratio=.2, critical_ratio=.4,
                               minimum_absolute_deviation=5)
        history = [100 + (index % 7) * 5 for index in range(28)]
        for current in (20, 220):
            with self.subTest(current=current):
                self.assertEqual(assess(timed_series([*history, current]), policy).status,
                                 AnomalyStatus.ANOMALY)
        self.assertEqual(assess(timed_series([*history, 103]), policy).status, AnomalyStatus.NORMAL)

    def test_rate_requires_current_denominator(self):
        values = [.4] * 8
        low = timed_series(values, "view_to_cart_rate", samples=(200,) * 7 + (20,))
        enough = replace(low, current_sample_size=200)
        self.assertEqual(assess(low, policy_for(low.metric_name, 7)).status,
                         AnomalyStatus.INSUFFICIENT_HISTORY)
        self.assertEqual(assess(enough, policy_for(enough.metric_name, 7)).status, AnomalyStatus.NORMAL)

    def test_rate_excludes_low_denominator_training_points(self):
        values = [.4] * 9
        item = timed_series(values, "view_to_cart_rate", samples=(10,) + (200,) * 8)
        result = assess(item, policy_for(item.metric_name, 7))
        self.assertEqual(result.status, AnomalyStatus.NORMAL)
        self.assertEqual(result.history_count, 7)
        self.assertEqual(result.details["raw_history_count"], 8)

    def test_operational_confidence_mapping(self):
        result = assess(timed_series([100] * 57), AnomalyPolicy())
        self.assertEqual(result.confidence, BaselineConfidence.HIGH)
        self.assertEqual(result.details["confidence"], "HIGH")

    def test_metric_specific_configuration_is_explicit(self):
        self.assertEqual(policy_for("gross_revenue", 7).baseline_strategies,
                         ("seasonal_trend", "day_of_week", "trend", "robust_history"))
        self.assertEqual(policy_for("completed_order_volume", 7).baseline_strategies,
                         ("day_of_week", "trend", "robust_history"))
        self.assertEqual(policy_for("failed_check_count", 7).baseline_strategies,
                         ("robust_history",))
        self.assertEqual(policy_for("view_to_cart_rate", 7).minimum_sample_size, 100)


class BacktestTests(unittest.TestCase):
    def test_stable_weekly_trend_and_noisy_healthy_scenarios_are_sensible(self):
        stable = backtest_series(timed_series([100] * 40), AnomalyPolicy())
        self.assertEqual(stable.anomaly_count, 0)
        trend_values = [100 + 3 * index for index in range(40)]
        trend = backtest_series(timed_series(trend_values),
                                AnomalyPolicy(baseline_strategies=("trend", "robust_history")))
        self.assertEqual(trend.anomaly_count, 0)
        pattern = (0, 10, 20, 5, -5, -15, -10)
        weekly = [100 + pattern[index % 7] for index in range(50)]
        report = backtest_series(timed_series(weekly),
                                 AnomalyPolicy(baseline_strategies=("day_of_week", "robust_history")))
        self.assertEqual(report.results[-1].baseline_strategy, "day_of_week")
        self.assertEqual(report.results[-1].status, AnomalyStatus.NORMAL)
        noisy = backtest_series(timed_series([100 + (-1) ** index * (index % 3) for index in range(40)]),
                                   AnomalyPolicy())
        self.assertLessEqual(noisy.anomaly_count, 1)

    def test_walk_forward_has_no_future_leakage(self):
        initial = timed_series([10, 10, 10, 10, 10, 10, 10, 10])
        extended = timed_series([10, 10, 10, 10, 10, 10, 10, 10, 1000])
        first = backtest_series(initial, AnomalyPolicy()).results
        second = backtest_series(extended, AnomalyPolicy()).results[:len(first)]
        self.assertEqual([(r.status, r.expected_value) for r in first],
                         [(r.status, r.expected_value) for r in second])

    def test_single_spike_drop_and_persistent_level_shift_are_visible(self):
        policy = AnomalyPolicy(warning_ratio=.2, critical_ratio=.4)
        for abnormal in (40, 180):
            report = backtest_series(timed_series([100] * 14 + [abnormal]), policy,
                                     truth_labels=(False,) * 14 + (True,))
            self.assertEqual(report.results[-1].status, AnomalyStatus.ANOMALY)
            self.assertEqual(report.false_positive_proxy, 0)
        shifted = backtest_series(timed_series([100] * 14 + [150] * 5), policy)
        self.assertGreaterEqual(shifted.anomaly_count, 5)


class ExplainCliTests(unittest.TestCase):
    def test_explain_prints_selected_strategy_and_bounds(self):
        item = timed_series([10 + index for index in range(15)])
        output = []
        with patch("src.quality.anomaly_sources.load_metric_series", return_value=[item]), \
             patch("builtins.print", side_effect=output.append):
            self.assertEqual(explain_main(["explain", "gross_revenue"]), 0)
        payload = json.loads(output[0])
        self.assertEqual(payload["baseline_strategy"], "trend")
        self.assertIn("lower_bound", payload)
        self.assertIn("explanation", payload)


if __name__ == "__main__":
    unittest.main()
