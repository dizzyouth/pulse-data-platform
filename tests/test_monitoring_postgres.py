"""Opt-in real PostgreSQL tests, isolated in a disposable database we create."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import os
from pathlib import Path
import unittest
from unittest.mock import patch
from uuid import uuid4

import psycopg
from psycopg import sql

from src.quality.execution import ExecutionContext
from src.quality.models import Severity, Status, summarize
from src.quality.persistence import ensure_monitoring_schema, persist_quality_run, PersistenceError
from src.warehouse.load_gold import connection_kwargs
from tests.test_quality import sample_result
from tests.test_quality_persistence import fixture_run


@unittest.skipUnless(os.environ.get("RUN_MONITORING_INTEGRATION_TESTS") == "1",
                     "Requires PostgreSQL with CREATE DATABASE permission; set RUN_MONITORING_INTEGRATION_TESTS=1")
class MonitoringPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = connection_kwargs()
        cls.database = "pulse_quality_test_" + uuid4().hex
        with psycopg.connect(**cls.base, autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cls.database)))
        cls.config = {**cls.base, "dbname": cls.database}
        cls.override = patch("src.quality.persistence.connection_kwargs", return_value=cls.config)
        cls.override.start()
        cls.addClassCleanup(cls.cleanup_database)
        ensure_monitoring_schema()

    @classmethod
    def cleanup_database(cls):
        cls.override.stop()
        with psycopg.connect(**cls.base, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(cls.database)))

    def context(self):
        return ExecutionContext(execution_id=str(uuid4()))

    def fetch(self, statement, params=()):
        with psycopg.connect(**self.config) as connection:
            return connection.execute(statement, params).fetchall()

    def test_repeatable_schema_creation_indexes_and_foreign_key(self):
        ensure_monitoring_schema()
        ensure_monitoring_schema()
        tables = self.fetch("SELECT tablename FROM pg_tables WHERE schemaname='monitoring'")
        self.assertEqual({row[0] for row in tables}, {"quality_runs", "quality_results"})
        indexes = self.fetch("SELECT indexname FROM pg_indexes WHERE schemaname='monitoring'")
        self.assertTrue({"quality_runs_completed_idx", "quality_runs_dataset_idx", "quality_runs_layer_idx",
                         "quality_runs_status_idx", "quality_results_critical_idx"} <= {row[0] for row in indexes})
        with psycopg.connect(**self.config) as connection, self.assertRaises(psycopg.errors.ForeignKeyViolation):
            connection.execute("""INSERT INTO monitoring.quality_results VALUES
                (%s,%s,'orphan','count','FAIL','CRITICAL','0','1',now(),'{}')""", (uuid4(), uuid4()))

    def test_pass_warn_fail_persist_with_matching_summary_and_check_details(self):
        for status, severity in ((Status.PASS, Severity.CRITICAL), (Status.WARN, Severity.WARNING),
                                 (Status.FAIL, Severity.CRITICAL)):
            with self.subTest(status=status):
                run = fixture_run(sample_result(status, severity))
                identity = persist_quality_run(run, self.context())
                summary = summarize(run.results)
                actual = self.fetch("""SELECT overall_status, total_checks, passed_checks, warning_checks,
                    failed_checks, critical_failures, should_block FROM monitoring.quality_runs
                    WHERE quality_run_id=%s""", (identity,))[0]
                self.assertEqual(actual, (summary.overall_status, summary.total_checks, summary.passed,
                                         summary.warnings, summary.failed, summary.critical_failures,
                                         summary.critical_failures > 0))
                checks = self.fetch("SELECT check_name,status,severity,observed_value,expected_value,details "
                                    "FROM monitoring.quality_results WHERE quality_run_id=%s", (identity,))
                self.assertEqual(len(checks), 1)
                self.assertEqual(checks[0][:5], ("sample", status, severity, 1, 1))
                self.assertIn("message", checks[0][5])

    def test_jsonb_preserves_structured_boolean_numeric_text_and_null_values(self):
        values = [True, 2.5, "2.5", None, {"min": 0, "values": [1, False]}]
        results = [replace(sample_result(), check_name=f"json_{i}", observed_value=value, expected_value=value)
                   for i, value in enumerate(values)]
        identity = persist_quality_run(fixture_run(*results), self.context())
        rows = self.fetch("SELECT observed_value,expected_value FROM monitoring.quality_results "
                          "WHERE quality_run_id=%s ORDER BY check_name", (identity,))
        self.assertEqual(rows, [(value, value) for value in values])
        self.assertIs(rows[0][0], True)

    def test_retry_identity_replaces_only_same_attempt_and_preserves_prior_failures(self):
        context = self.context()
        failed = fixture_run(sample_result(Status.FAIL))
        identity = persist_quality_run(failed, context)
        self.assertEqual(persist_quality_run(failed, context), identity)
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.quality_results WHERE quality_run_id=%s", (identity,))[0][0], 1)
        second = persist_quality_run(fixture_run(), replace(context, attempt_number=2))
        self.assertNotEqual(identity, second)
        self.assertEqual(self.fetch("SELECT overall_status FROM monitoring.quality_runs WHERE quality_run_id=%s", (identity,))[0][0], "FAIL")
        replacement = fixture_run(replace(sample_result(), check_name="new_check"))
        persist_quality_run(replacement, replace(context, attempt_number=2))
        self.assertEqual(self.fetch("SELECT check_name FROM monitoring.quality_results WHERE quality_run_id=%s", (second,)), [("new_check",)])
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.quality_runs WHERE execution_id=%s", (context.execution_id,))[0][0], 2)

    def test_result_failure_rolls_back_new_run_and_prior_run_replacement(self):
        context = self.context()
        identity = persist_quality_run(fixture_run(sample_result(Status.FAIL)), context)
        with psycopg.connect(**self.config) as connection:
            connection.execute("ALTER TABLE monitoring.quality_results ADD CONSTRAINT reject_test_result CHECK (check_name <> 'explode')")
        try:
            invalid = fixture_run(replace(sample_result(), check_name="explode"))
            for attempted_context in (context, replace(context, attempt_number=2)):
                with self.assertRaises(PersistenceError):
                    persist_quality_run(invalid, attempted_context)
            self.assertEqual(self.fetch("SELECT overall_status FROM monitoring.quality_runs WHERE quality_run_id=%s", (identity,)), [("FAIL",)])
            self.assertEqual(self.fetch("SELECT check_name FROM monitoring.quality_results WHERE quality_run_id=%s", (identity,)), [("sample",)])
            self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.quality_runs WHERE execution_id=%s", (context.execution_id,)), [(1,)])
        finally:
            with psycopg.connect(**self.config) as connection:
                connection.execute("ALTER TABLE monitoring.quality_results DROP CONSTRAINT reject_test_result")

    def test_concurrent_rewrites_leave_one_consistent_run(self):
        context = self.context()
        runs = [fixture_run(replace(sample_result(), check_name=f"parallel_{i}")) for i in range(2)]
        with ThreadPoolExecutor(max_workers=2) as pool:
            identities = list(pool.map(lambda run: persist_quality_run(run, context), runs))
        self.assertEqual(identities[0], identities[1])
        self.assertEqual(self.fetch("SELECT count(*) FROM monitoring.quality_results WHERE quality_run_id=%s", (identities[0],)), [(1,)])

    def test_airflow_context_is_saved_and_query_examples_execute(self):
        run = fixture_run()
        context = ExecutionContext(execution_source="airflow", execution_id="manual__" + uuid4().hex,
                                   dag_id="pulse", airflow_run_id="airflow-run", task_id="quality_check_silver",
                                   attempt_number=2, map_index=3, logical_date_utc=run.started_at_utc)
        identity = persist_quality_run(run, context)
        row = self.fetch("SELECT dag_id,airflow_run_id,task_id,attempt_number,map_index,logical_date_utc "
                         "FROM monitoring.quality_runs WHERE quality_run_id=%s", (identity,))[0]
        self.assertEqual(row, (context.dag_id, context.airflow_run_id, context.task_id, 2, 3, context.logical_date_utc))
        path = Path(__file__).resolve().parents[1] / "monitoring/queries.sql"
        with psycopg.connect(**self.config) as connection:
            for statement in path.read_text(encoding="utf-8").split(";"):
                if statement.strip():
                    connection.execute(statement).fetchall()


if __name__ == "__main__":
    unittest.main()
