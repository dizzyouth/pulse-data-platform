"""Deterministic quality contracts and broker-free Spark snapshot tests."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.analytics.gold_build import (
    GoldTables, SILVER_VALID_SCHEMA, build_gold_spark_session, build_gold_tables,
)
from src.streaming.silver_streaming import BRONZE_VALID_SCHEMA, classify_silver_events
from src.quality.checks import validate_rule
from src.quality.datasets import gold_rules, silver_rules
from src.quality.models import (
    AllowedValues, CheckDetails, Freshness, NullRatio, NumericBounds, Pattern,
    QualityContext, QualityResult, RowCount, Severity, Status, Uniqueness,
    VolumeChange, report_json, should_block, summarize,
)
from src.quality.reconciliation import reconcile_bronze_silver, reconcile_silver_gold
from src.quality.runner import main, run_quality_checks
from tests.test_silver_streaming import bronze_record
from tests.test_gold_build import realistic_events, silver_event

from pyspark.sql import functions as F


NOW = datetime(2026, 1, 2, 12, tzinfo=timezone.utc)
CONTEXT = QualityContext(dataset_name="fixture", layer="silver", checked_at_utc=NOW)


def sample_result(status=Status.PASS, severity=Severity.CRITICAL):
    return QualityResult(check_name="sample", dataset_name="fixture", layer="silver",
                         status=status, severity=severity, metric_name="row_count",
                         observed_value=1, expected_value=1, checked_at_utc=NOW)


class QualityModelTests(unittest.TestCase):
    def test_summary_and_blocking_separate_status_from_severity(self):
        results = [sample_result(), sample_result(Status.WARN, Severity.INFO),
                   sample_result(Status.FAIL, Severity.WARNING)]
        summary = summarize(results)
        self.assertEqual((summary.total_checks, summary.passed, summary.warnings, summary.failed), (3, 1, 1, 1))
        self.assertEqual(summary.overall_status, Status.WARN)
        self.assertFalse(should_block(results))
        results.append(sample_result(Status.FAIL))
        self.assertEqual(summarize(results).critical_failures, 1)
        self.assertEqual(summarize(results).overall_status, Status.FAIL)
        self.assertTrue(should_block(iter(results)))

    def test_empty_summary_is_pass_with_zero_checks(self):
        self.assertEqual(summarize([]).total_checks, 0)
        self.assertEqual(summarize([]).overall_status, Status.PASS)
        self.assertFalse(should_block([]))

    def test_context_requires_aware_time_and_nonnegative_reference(self):
        for updates in ({"checked_at_utc": NOW.replace(tzinfo=None)}, {"reference_count": -1},
                        {"reference_count": 1.5}, {"dataset_name": ""}):
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                replace(CONTEXT, **updates)
        shifted = NOW.astimezone(timezone(timedelta(hours=5)))
        self.assertEqual(replace(CONTEXT, checked_at_utc=shifted).checked_at_utc, NOW)

    def test_invalid_rule_configuration_is_rejected(self):
        rules = [RowCount(check_name="x", min_rows=-1), NullRatio(check_name="x", column="id", max_ratio=1.1),
                 Uniqueness(check_name="x", columns=()), NumericBounds(check_name="x", column="amount"),
                 NumericBounds(check_name="x", column="amount", minimum=2, maximum=1),
                 Freshness(check_name="x", column="ts", max_age_seconds=float("nan")),
                 VolumeChange(check_name="x", max_deviation_ratio=-0.1),
                 AllowedValues(check_name="x", column="id", values=()),
                 RowCount(check_name=" "), Pattern(check_name="x", column="id", pattern="")]
        for rule in rules:
            with self.subTest(rule=rule), self.assertRaises(ValueError):
                validate_rule(rule)

    def test_json_report_is_structured_and_has_utc_timestamps(self):
        result = replace(sample_result(), details=CheckDetails(latest_timestamp_utc=NOW))
        report = json.loads(report_json([result]))
        self.assertEqual(report["summary"]["overall_status"], "PASS")
        self.assertEqual(report["results"][0]["severity"], "CRITICAL")
        self.assertEqual(report["results"][0]["checked_at_utc"], NOW.isoformat())
        self.assertEqual(report["results"][0]["details"]["latest_timestamp_utc"], NOW.isoformat())

    def test_result_rejects_invalid_status_and_nonfinite_observations(self):
        for updates in ({"status": "UNKNOWN"}, {"severity": "HIGH"},
                        {"observed_value": float("inf")}, {"checked_at_utc": NOW.replace(tzinfo=None)}):
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                replace(sample_result(), **updates)

    def test_cli_volume_threshold_requires_a_reference(self):
        with patch("sys.stderr"), self.assertRaises(SystemExit) as raised:
            main(["silver_valid", "--max-volume-change", "0.2"])
        self.assertEqual(raised.exception.code, 2)


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS", "1") == "1", "Spark tests disabled")
class QualitySparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = build_gold_spark_session(app_name="pulse-quality-tests", master="local[1]")
        cls.spark.conf.set("spark.sql.shuffle.partitions", "2")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, rows=None):
        return self.spark.createDataFrame(
            [("a", "x", NOW, "view", 2.0, "US"), ("b", "y", NOW, "pay", 3.0, None)] if rows is None else rows,
            "id string, business_key string, ts timestamp, kind string, amount double, country string",
        )

    def run_rules(self, frame, *rules, context=CONTEXT):
        return run_quality_checks(frame, rules, context)

    def test_all_check_types_pass_on_a_spark_dataframe(self):
        results = self.run_rules(self.frame(),
            RowCount(check_name="rows", min_rows=2), NullRatio(check_name="nulls", column="id"),
            Uniqueness(check_name="unique", columns=("id",)),
            AllowedValues(check_name="allowed", column="kind", values=("view", "pay")),
            NumericBounds(check_name="bounds", column="amount", minimum=0, maximum=3),
            Pattern(check_name="format", column="country", pattern="^[A-Z]{2}$"),
            Freshness(check_name="fresh", column="ts", max_age_seconds=60),
            VolumeChange(check_name="volume", max_deviation_ratio=0), context=replace(CONTEXT, reference_count=2))
        self.assertEqual(len(results), 8)
        self.assertTrue(all(r.status == Status.PASS and r.checked_at_utc == NOW for r in results))

    def test_null_ratio_reports_fraction_and_failure_at_threshold(self):
        results = self.run_rules(self.frame(),
            NullRatio(check_name="fail", column="country", max_ratio=0.49),
            NullRatio(check_name="boundary", column="country", max_ratio=0.5))
        self.assertEqual(results[0].observed_value, 0.5)
        self.assertEqual(results[0].details.violating_rows, 1)
        self.assertEqual([r.status for r in results], [Status.FAIL, Status.PASS])

    def test_duplicate_ids_and_composite_null_keys_report_excess_rows(self):
        frame = self.frame([("a", None, NOW, "view", 1.0, "US")] * 3 + [("a", "x", NOW, "view", 1.0, "US")])
        results = self.run_rules(frame, Uniqueness(check_name="id", columns=("id",)),
                                 Uniqueness(check_name="composite", columns=("id", "business_key")))
        self.assertEqual([r.details.duplicate_count for r in results], [3, 2])
        self.assertEqual([r.details.duplicate_rate for r in results], [0.75, 0.5])
        self.assertTrue(all(r.status == Status.FAIL for r in results))

    def test_allowed_values_and_null_policy(self):
        frame = self.frame().withColumn("kind", F.lit(None).cast("string"))
        results = self.run_rules(frame,
            AllowedValues(check_name="required", column="kind", values=("view",)),
            AllowedValues(check_name="optional", column="kind", values=("view",), allow_null=True))
        self.assertEqual([r.status for r in results], [Status.FAIL, Status.PASS])
        invalid = self.run_rules(self.frame(), AllowedValues(check_name="values", column="kind", values=("view",)))[0]
        self.assertEqual(invalid.observed_value, 1)

    def test_numeric_bounds_reject_negative_high_nan_and_infinity(self):
        frame = self.frame([(str(i), "x", NOW, "view", value, None) for i, value in
                            enumerate([-1.0, 0.0, 2.0, float("nan"), float("inf"), float("-inf"), None])])
        results = self.run_rules(frame,
            NumericBounds(check_name="optional", column="amount", minimum=0, maximum=1),
            NumericBounds(check_name="required", column="amount", minimum=0, maximum=1, allow_null=False),
            NumericBounds(check_name="positive", column="amount", minimum=0, maximum=1, minimum_inclusive=False))
        self.assertEqual([r.observed_value for r in results], [5, 6, 6])
        self.assertTrue(should_block(results))

    def test_pattern_failure(self):
        frame = self.frame().withColumn("country", F.lit("usa"))
        result = self.run_rules(frame, Pattern(check_name="country", column="country", pattern="^[A-Z]{2}$"))[0]
        self.assertEqual(result.observed_value, 2)
        self.assertEqual(result.status, Status.FAIL)

    def test_freshness_old_future_and_null_timestamps(self):
        rule = Freshness(check_name="freshness", column="ts", max_age_seconds=60)
        for value, expected_age in ((NOW - timedelta(seconds=61), 61), (NOW + timedelta(seconds=1), -1), (None, None)):
            with self.subTest(value=value):
                frame = self.frame([("a", "x", value, "view", 1.0, "US")])
                result = self.run_rules(frame, rule)[0]
                self.assertEqual(result.status, Status.FAIL)
                self.assertEqual(result.observed_value, expected_age)

    def test_freshness_uses_latest_instant_independent_of_session_timezone(self):
        frame = self.frame([("a", "x", NOW - timedelta(days=2), "view", 1.0, "US"),
                            ("b", "y", NOW - timedelta(seconds=60), "view", 1.0, "US"),
                            ("c", "z", None, "view", 1.0, "US")])
        self.spark.conf.set("spark.sql.session.timeZone", "America/New_York")
        try:
            result = self.run_rules(frame, Freshness(check_name="fresh", column="ts", max_age_seconds=60))[0]
            self.assertEqual(result.observed_value, 60)
            self.assertEqual(result.status, Status.PASS)
            self.assertEqual(result.details.latest_timestamp_utc, NOW - timedelta(seconds=60))
        finally:
            self.spark.conf.set("spark.sql.session.timeZone", "UTC")

    def test_volume_change_severity_and_missing_or_zero_baseline(self):
        rules = [VolumeChange(check_name=severity.value, severity=severity, max_deviation_ratio=0.25) for severity in Severity]
        results = self.run_rules(self.frame(), *rules, context=replace(CONTEXT, reference_count=4))
        self.assertEqual([r.status for r in results], [Status.WARN, Status.WARN, Status.FAIL])
        self.assertTrue(all(r.observed_value == 0.5 for r in results))
        missing = self.run_rules(self.frame(), rules[-1])[0]
        self.assertEqual(missing.status, Status.WARN)
        self.assertFalse(should_block([missing]))
        zero = replace(CONTEXT, reference_count=0)
        self.assertEqual(self.run_rules(self.frame(), rules[-1], context=zero)[0].status, Status.FAIL)
        self.assertEqual(self.run_rules(self.frame([]), rules[-1], context=zero)[0].status, Status.PASS)
        self.assertEqual(self.run_rules(self.frame([]), rules[-1], context=replace(CONTEXT, reference_count=2))[0].observed_value, 1)
        json.loads(report_json(self.run_rules(self.frame(), rules[-1], context=zero)))

    def test_empty_data_is_not_evidence_of_completeness_or_freshness(self):
        results = self.run_rules(self.frame([]), RowCount(check_name="rows", min_rows=1),
            NullRatio(check_name="nulls", column="id"), Uniqueness(check_name="unique", columns=("id",)),
            Freshness(check_name="fresh", column="ts", max_age_seconds=10))
        self.assertEqual([r.status for r in results], [Status.FAIL, Status.WARN, Status.WARN, Status.FAIL])
        self.assertEqual(results[1].metric_name, "null_ratio")
        self.assertEqual(results[2].metric_name, "duplicate_ratio")
        self.assertIsNone(results[1].observed_value)
        self.assertIsNone(results[2].observed_value)

    def test_missing_columns_and_wrong_types_are_results_not_silent_skips(self):
        results = self.run_rules(self.frame(), NullRatio(check_name="missing", column="absent"),
            Freshness(check_name="wrong_time", column="kind", max_age_seconds=1),
            NumericBounds(check_name="wrong_numeric", column="kind", minimum=0),
            RowCount(check_name="still_runs"))
        self.assertEqual([r.status for r in results], [Status.FAIL, Status.FAIL, Status.FAIL, Status.PASS])
        self.assertIn("Missing columns", results[0].details.message)

    def test_spark_execution_errors_are_not_converted_to_quality_passes(self):
        frame = self.frame()
        with patch.object(frame, "agg", side_effect=RuntimeError("storage unavailable")):
            with self.assertRaisesRegex(RuntimeError, "storage unavailable"):
                self.run_rules(frame, RowCount(check_name="rows"))

    def test_literal_column_names_and_duplicate_rule_names(self):
        frame = self.frame().withColumnRenamed("id", "event.id")
        self.assertEqual(self.run_rules(frame, NullRatio(check_name="literal", column="event.id"))[0].status, Status.PASS)
        with self.assertRaisesRegex(ValueError, "unique"):
            self.run_rules(frame, RowCount(check_name="same"), RowCount(check_name="same"))
        self.assertEqual(self.run_rules(frame), [])

    def test_streaming_inputs_are_rejected_without_starting_queries(self):
        stream = self.spark.readStream.format("rate").load()
        with self.assertRaisesRegex(ValueError, "bounded batch"):
            self.run_rules(stream, RowCount(check_name="rows"))

    def test_silver_policy_matches_transformed_valid_data_and_catches_drift(self):
        bronze = self.spark.createDataFrame([bronze_record(), bronze_record(event_id="evt_2", quantity=None,
                                                                          unit_price=None, country=None, currency=None)], BRONZE_VALID_SCHEMA)
        valid = classify_silver_events(bronze, deduplicate=False).valid
        self.assertEqual(summarize(run_quality_checks(valid, silver_rules(), CONTEXT)).overall_status, Status.PASS)
        corrupted = valid.withColumn("quantity", F.lit(0)).withColumn("country", F.lit("USA"))
        results = {r.check_name: r for r in run_quality_checks(corrupted, silver_rules(), CONTEXT)}
        self.assertEqual(results["quantity_positive"].status, Status.FAIL)
        self.assertEqual(results["country_format"].status, Status.FAIL)

    def test_gold_policies_match_existing_aggregations(self):
        tables = build_gold_tables(self.spark.createDataFrame(realistic_events(), SILVER_VALID_SCHEMA))
        for name in ("daily_sales", "customer_metrics", "product_metrics", "funnel_metrics"):
            with self.subTest(name=name):
                results = run_quality_checks(getattr(tables, name), gold_rules(name), replace(CONTEXT, layer="gold", dataset_name=name))
                self.assertEqual(summarize(results).overall_status, Status.PASS)

    def test_gold_grain_nullability_and_numeric_failures(self):
        daily = self.spark.createDataFrame([(None, None, None, -1, -2, -3.0, None)],
            "event_date date, country string, currency string, completed_orders long, units_sold long, gross_revenue double, avg_order_value double")
        gold = {r.check_name: r for r in run_quality_checks(daily, gold_rules("daily_sales"), CONTEXT)}
        self.assertEqual(gold["event_date_complete"].status, Status.FAIL)
        self.assertEqual(gold["country_complete"].status, Status.WARN)
        self.assertEqual(gold["currency_complete"].status, Status.WARN)
        self.assertEqual(gold["gross_revenue_nonnegative"].status, Status.FAIL)
        analytics = {r.check_name: r for r in run_quality_checks(daily, gold_rules("daily_sales", layer="analytics"), CONTEXT)}
        self.assertEqual(analytics["country_complete"].status, Status.FAIL)

    def test_gold_funnel_policy_rejects_out_of_range_rates(self):
        tables = build_gold_tables(self.spark.createDataFrame(realistic_events(), SILVER_VALID_SCHEMA))
        corrupted = tables.funnel_metrics.withColumn("view_to_cart_rate", F.lit(1.1))
        results = {r.check_name: r for r in run_quality_checks(corrupted, gold_rules("funnel_metrics"), CONTEXT)}
        self.assertEqual(results["view_to_cart_rate_range"].status, Status.FAIL)

    def test_snapshot_reconciliation_accounts_for_duplicates_and_rejects(self):
        bronze = self.spark.createDataFrame([bronze_record(), bronze_record(kafka_offset=18),
                                            bronze_record(event_id="bad", quantity=0)], BRONZE_VALID_SCHEMA)
        frames = classify_silver_events(bronze, deduplicate=False)
        valid = frames.valid.dropDuplicates(["event_id"])
        results = reconcile_bronze_silver(bronze, valid, frames.rejected, CONTEXT, bounded_snapshot=True)
        self.assertTrue(all(r.status == Status.PASS for r in results))
        self.assertEqual(results[0].details.deduplicated_rows, 1)
        self.assertEqual(results[0].details.reference_count, 3)
        missing = reconcile_bronze_silver(bronze, valid.limit(0), frames.rejected, CONTEXT, bounded_snapshot=True)
        self.assertTrue(should_block(missing))
        excess = reconcile_bronze_silver(bronze, valid, frames.rejected.unionByName(frames.rejected), CONTEXT, bounded_snapshot=True)
        self.assertEqual(excess[1].status, Status.FAIL)
        with self.assertRaisesRegex(ValueError, "bounded_snapshot"):
            reconcile_bronze_silver(bronze, valid, frames.rejected, CONTEXT)

    def test_gold_reconciliation_handles_no_payments_and_no_product(self):
        silver = self.spark.createDataFrame([silver_event("view", "product_viewed", product_id=None)], SILVER_VALID_SCHEMA)
        tables = build_gold_tables(silver)
        results = reconcile_silver_gold(silver, tables, CONTEXT, bounded_snapshot=True)
        self.assertTrue(all(r.status == Status.PASS for r in results))
        self.assertEqual([r.expected_value for r in results], [0, 1, 0, 1])
        missing = GoldTables(tables.daily_sales, tables.customer_metrics.limit(0), tables.product_metrics, tables.funnel_metrics)
        self.assertTrue(should_block(reconcile_silver_gold(silver, missing, CONTEXT, bounded_snapshot=True)))

    def test_gold_reconciliation_detects_all_missing_eligible_outputs(self):
        silver = self.spark.createDataFrame([silver_event("paid", "payment_completed", quantity=1, unit_price=2.0)], SILVER_VALID_SCHEMA)
        tables = build_gold_tables(silver)
        empty = GoldTables(*(getattr(tables, name).limit(0) for name in
                             ("daily_sales", "customer_metrics", "product_metrics", "funnel_metrics")))
        results = reconcile_silver_gold(silver, empty, CONTEXT, bounded_snapshot=True)
        self.assertEqual([r.status for r in results], [Status.FAIL] * 4)
        self.assertTrue(should_block(results))

    def test_cli_reads_temporary_parquet_and_blocking_is_opt_in(self):
        with TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            frame = self.spark.createDataFrame([bronze_record()], BRONZE_VALID_SCHEMA)
            path = Path(directory) / "silver"
            classify_silver_events(frame, deduplicate=False).valid.withColumn("quantity", F.lit(0)).write.parquet(str(path))
            # Reuse the test session; main must still call stop in its finally block.
            with patch("src.analytics.gold_build.build_gold_spark_session", return_value=self.spark), \
                 patch.object(self.spark, "stop") as stop, patch("builtins.print") as output:
                self.assertEqual(main(["silver_valid", "--path", str(path)]), 0)
                self.assertEqual(main(["silver_valid", "--path", str(path), "--block-on-critical"]), 1)
                self.assertEqual(stop.call_count, 2)
                self.assertEqual(json.loads(output.call_args.args[0])["summary"]["overall_status"], "FAIL")


if __name__ == "__main__":
    unittest.main()
