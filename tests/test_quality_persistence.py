"""CI-safe execution, persistence adapter and runner ordering contracts."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import unittest
from unittest.mock import MagicMock, patch

from src.quality.execution import DatasetQualityRun, ExecutionContext, execution_context
from src.quality.models import Severity, Status
from src.quality.persistence import PersistenceError, json_value, persist_quality_run
from src.quality.runner import main
from tests.test_quality import sample_result


def fixture_run(*results):
    start = datetime.now(timezone.utc)
    return DatasetQualityRun(dataset_name="fixture", layer="silver", started_at_utc=start,
                             completed_at_utc=start + timedelta(seconds=1),
                             results=results or (sample_result(),))


class ExecutionContextTests(unittest.TestCase):
    def test_local_identity_is_optional_and_new_by_default(self):
        a, b = execution_context(environ={}), execution_context(environ={})
        self.assertNotEqual(a.execution_id, b.execution_id)
        self.assertEqual(a.execution_source, "cli")
        self.assertIsNone(a.dag_id)
        self.assertEqual(execution_context(execution_id="repeat", environ={}),
                         execution_context(execution_id="repeat", environ={}))

    def test_airflow_context_and_idempotent_identity_include_attempt_and_dataset(self):
        env = {"AIRFLOW_CTX_DAG_ID": "pulse", "AIRFLOW_CTX_DAG_RUN_ID": "manual__test",
               "AIRFLOW_CTX_TASK_ID": "quality_check_gold", "QUALITY_ATTEMPT_NUMBER": "2",
               "QUALITY_MAP_INDEX": "-1", "QUALITY_LOGICAL_DATE": "2026-01-01T00:00:00+00:00"}
        context = execution_context(environ=env)
        self.assertEqual((context.dag_id, context.airflow_run_id, context.task_id, context.attempt_number),
                         ("pulse", "manual__test", "quality_check_gold", 2))
        self.assertEqual(context.logical_date_utc.year, 2026)
        identity = context.run_id("daily_sales", "gold")
        self.assertEqual(identity, execution_context(environ=env).run_id("daily_sales", "gold"))
        for altered in (replace(context, attempt_number=3), replace(context, task_id="other"),
                        replace(context, map_index=0), replace(context, airflow_run_id="other")):
            self.assertNotEqual(identity, altered.run_id("daily_sales", "gold"))
        self.assertNotEqual(identity, context.run_id("product_metrics", "gold"))
        self.assertNotEqual(identity, context.run_id("daily_sales", "analytics"))

    def test_incomplete_or_ambiguous_context_fails(self):
        with self.assertRaises(ValueError):
            execution_context(environ={"AIRFLOW_CTX_DAG_ID": "pulse"})
        for updates in ({"attempt_number": 0}, {"map_index": -2},
                        {"logical_date_utc": datetime(2026, 1, 1)}, {"execution_id": ""}):
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                replace(ExecutionContext(execution_id="x"), **updates)

    def test_dataset_envelope_rejects_mixed_datasets_duplicates_and_bad_timing(self):
        run = fixture_run()
        for updates in ({"results": (sample_result(), sample_result())},
                        {"results": (replace(sample_result(), layer="gold"),)},
                        {"completed_at_utc": run.started_at_utc - timedelta(seconds=1)}):
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                replace(run, **updates)

    def test_json_values_keep_types_and_reject_nonfinite_numbers(self):
        for value in (None, True, 3, 1.5, "1.5", {"minimum": 0, "allowed": [True, None]}):
            self.assertEqual(json_value(value), value)
            self.assertEqual(type(json_value(value)), type(value))
        with self.assertRaises(ValueError):
            json_value(float("inf"))


class PersistenceAdapterTests(unittest.TestCase):
    def test_adapter_persists_matching_summary_and_all_jsonb_results(self):
        run = fixture_run(sample_result(), replace(sample_result(Status.WARN, Severity.WARNING), check_name="warn"),
                          replace(sample_result(Status.FAIL), check_name="fail"))
        context = ExecutionContext(execution_id="fixture")
        with patch("src.quality.persistence.ensure_monitoring_schema") as ensure, \
             patch("src.quality.persistence.psycopg.connect") as connect:
            identity = persist_quality_run(run, context)
        ensure.assert_called_once()
        cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        params = cursor.execute.call_args_list[0].args[1]
        self.assertEqual(params[13:], ("FAIL", 3, 1, 1, 1, 1, True))
        rows = cursor.executemany.call_args.args[1]
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(row[1] == identity for row in rows))
        self.assertEqual(rows[0][6].obj, 1)
        self.assertIsInstance(rows[0][9].obj, dict)
        connect.return_value.__exit__.assert_called_once_with(None, None, None)

    def test_write_error_is_sanitized_and_exits_transaction_with_error(self):
        with patch("src.quality.persistence.ensure_monitoring_schema"), \
             patch("src.quality.persistence.psycopg.connect") as connect:
            cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
            cursor.executemany.side_effect = RuntimeError("secret-connection-detail")
            with self.assertRaises(PersistenceError) as error:
                persist_quality_run(fixture_run(), ExecutionContext(execution_id="x"))
        self.assertNotIn("secret", str(error.exception))
        self.assertIs(connect.return_value.__exit__.call_args.args[0], RuntimeError)


class PersistenceRunnerTests(unittest.TestCase):
    def run_cli(self, severity, status, *, persist=True, failing_sink=False):
        result = replace(sample_result(status, severity), dataset_name="silver_valid")
        order = []
        def save(run, context):
            order.append("persist")
            self.assertEqual(run.results, (result,))
            self.assertEqual(context.execution_source, "cli")
            if failing_sink:
                raise PersistenceError("Persistence unavailable")
            return context.run_id(run.dataset_name, run.layer)
        def output(text, **_kwargs):
            order.append(json.loads(text)["event"])
        args = ["silver", "--block-on-critical", "--log-format", "jsonl"]
        if persist:
            args += ["--persist", "--execution-id", "test"]
        with patch.dict("os.environ", {}, clear=True), \
             patch("src.analytics.gold_build.build_gold_spark_session"), \
             patch("src.utils.parquet.read_parquet_data_files"), \
             patch("src.quality.runner.run_quality_checks", return_value=[result]), \
             patch("src.quality.persistence.persist_quality_run", side_effect=save) as sink, \
             patch("builtins.print", side_effect=output):
            if failing_sink:
                with self.assertRaises(PersistenceError):
                    main(args)
                self.assertNotIn("quality_result", order)
                self.assertEqual(order[-2:], ["quality_execution_error", "quality_summary"])
                return
            code = main(args)
        order.append("exit")
        self.assertEqual(code, int(status == Status.FAIL and severity == Severity.CRITICAL))
        if persist:
            self.assertEqual(order, ["persist", "quality_persisted", "quality_result", "quality_summary", "exit"])
        else:
            sink.assert_not_called()

    def test_critical_is_persisted_before_summary_and_blocking_exit(self):
        self.run_cli(Severity.CRITICAL, Status.FAIL)

    def test_pass_warning_and_info_are_persisted_without_blocking(self):
        for severity, status in ((Severity.CRITICAL, Status.PASS), (Severity.WARNING, Status.WARN),
                                 (Severity.INFO, Status.WARN)):
            with self.subTest(severity=severity):
                self.run_cli(severity, status)

    def test_local_default_never_calls_persistence(self):
        self.run_cli(Severity.CRITICAL, Status.PASS, persist=False)

    def test_persistence_failure_fails_task_even_for_passing_checks(self):
        self.run_cli(Severity.CRITICAL, Status.PASS, failing_sink=True)

    def test_gold_persists_four_distinct_dataset_runs(self):
        def assess(_frame, _rules, context):
            return [replace(sample_result(), dataset_name=context.dataset_name, layer=context.layer)]
        with patch.dict("os.environ", {}, clear=True), \
             patch("src.analytics.gold_build.build_gold_spark_session"), \
             patch("src.utils.parquet.read_parquet_data_files"), \
             patch("src.quality.runner.run_quality_checks", side_effect=assess), \
             patch("src.quality.persistence.persist_quality_run") as sink, patch("builtins.print"):
            self.assertEqual(main(["gold", "--persist"]), 0)
        self.assertEqual(sink.call_count, 4)
        self.assertEqual(len({call.args[0].dataset_name for call in sink.call_args_list}), 4)
        self.assertTrue(all(len(call.args[0].results) == 1 for call in sink.call_args_list))

    def test_identity_options_require_explicit_persistence(self):
        for args in (["silver", "--execution-id", "x"], ["silver", "--persist", "--attempt-number", "2"]):
            with self.subTest(args=args), patch("sys.stderr"), self.assertRaises(SystemExit):
                main(args)


if __name__ == "__main__":
    unittest.main()
