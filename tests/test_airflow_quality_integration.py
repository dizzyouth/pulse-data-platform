"""Opt-in real Airflow execution against an isolated SQLite metadata database.

Run in the Airflow image with RUN_AIRFLOW_INTEGRATION_TESTS=1 and a temporary
AIRFLOW_HOME / AIRFLOW__DATABASE__SQL_ALCHEMY_CONN, after `airflow db migrate`.
The production DAG graph, BashOperator, quality CLI and trigger rules execute;
only expensive data work and quality observations are replaced with fixtures.
Spark policy behavior is covered separately by test_quality_orchestration.
"""

import os
from pathlib import Path
import shlex
import textwrap
import unittest


@unittest.skipUnless(os.environ.get("RUN_AIRFLOW_INTEGRATION_TESTS") == "1",
                     "Requires Airflow image and isolated metadata DB; set RUN_AIRFLOW_INTEGRATION_TESTS=1")
class AirflowQualityExecutionTests(unittest.TestCase):
    def run_boundary(self, target, severity, status):
        from airflow.configuration import conf
        from airflow.models import DagBag
        from airflow.utils import timezone

        # These tests must never write runs into the running scheduler's database.
        self.assertTrue(conf.get("database", "sql_alchemy_conn").startswith("sqlite:///"))
        path = Path(__file__).resolve().parents[1] / "airflow/dags/pulse_analytics_pipeline.py"
        if not path.is_file():
            path = Path("/opt/airflow/dags/pulse_analytics_pipeline.py")
        self.assertTrue(path.is_file(), path)
        bag = DagBag(dag_folder=str(path), include_examples=False)
        self.assertFalse(bag.import_errors)
        dag = bag.get_dag("pulse_analytics_pipeline")
        dag.dag_id = f"phase53_{target}_{severity.lower()}"
        quality_id = f"quality_check_{target}"
        selected = dag.get_task(quality_id)
        args = shlex.split(selected.bash_command)[3:]
        code = textwrap.dedent(f"""
            from contextlib import ExitStack
            from dataclasses import replace
            from unittest.mock import patch
            from datetime import datetime, timezone
            import os
            from src.quality.models import QualityResult, Severity, Status
            from src.quality.runner import main
            result = QualityResult(check_name='boundary_fixture', dataset_name='fixture',
                layer={target!r}, status=Status({status!r}), severity=Severity({severity!r}),
                metric_name='row_count', observed_value=0, expected_value=1,
                checked_at_utc=datetime.now(timezone.utc))
            live = os.environ.get('RUN_MONITORING_INTEGRATION_TESTS') == '1'
            with ExitStack() as stack:
                stack.enter_context(patch('src.analytics.gold_build.build_gold_spark_session'))
                stack.enter_context(patch('src.utils.parquet.read_parquet_data_files'))
                stack.enter_context(patch('src.quality.runner.run_quality_checks',
                    side_effect=lambda frame, rules, ctx: [replace(result, dataset_name=ctx.dataset_name, layer=ctx.layer)]))
                snapshot = stack.enter_context(patch('src.quality.warehouse.warehouse_frames'))
                snapshot.return_value.__enter__.return_value = {{'daily_sales': object()}}
                if not live:
                    stack.enter_context(patch('src.quality.persistence.persist_quality_run',
                        side_effect=lambda run, ctx: ctx.run_id(run.dataset_name, run.layer)))
                else:
                    import psycopg
                    from src.warehouse.load_gold import connection_kwargs
                    from src.quality.observability import log_summary
                    def verified_summary(results, **kwargs):
                        if kwargs.get('completed', True):
                            with psycopg.connect(**connection_kwargs()) as connection:
                                count = connection.execute(
                                    'SELECT count(*) FROM monitoring.quality_runs WHERE dag_id=%s AND task_id=%s',
                                    (os.environ['AIRFLOW_CTX_DAG_ID'], os.environ['AIRFLOW_CTX_TASK_ID'])).fetchone()[0]
                                alerts = connection.execute(
                                    "SELECT count(*) FROM monitoring.alert_events WHERE source_type='QUALITY_FAILURE' "
                                    "AND dag_id=%s AND task_id=%s",
                                    (os.environ['AIRFLOW_CTX_DAG_ID'], os.environ['AIRFLOW_CTX_TASK_ID'])).fetchone()[0]
                            assert count > 0, 'Summary must follow committed persistence'
                            assert (alerts > 0) == ({severity!r} == 'CRITICAL'), \
                                'Quality alert must be committed before summary and blocking'
                        return log_summary(results, **kwargs)
                    stack.enter_context(patch('src.quality.observability.log_summary', verified_summary))
                raise SystemExit(main({args!r}))
        """)
        for task in dag.tasks:
            self.assertEqual(task.trigger_rule, "all_success")
            task.retries = 0  # Exercise terminal states without a retry delay.
            task.bash_command = "true"
        selected.bash_command = "python -c " + shlex.quote(code)
        run = dag.test(execution_date=timezone.datetime(2026, 1, 2))
        states = {ti.task_id: ti.state for ti in run.get_task_instances()}
        if os.environ.get("RUN_MONITORING_INTEGRATION_TESTS") == "1":
            import psycopg
            from src.warehouse.load_gold import connection_kwargs
            with psycopg.connect(**connection_kwargs()) as connection:
                rows = connection.execute("""SELECT r.overall_status, r.should_block, r.attempt_number,
                    r.airflow_run_id, r.logical_date_utc, count(q.quality_result_id)
                    FROM monitoring.quality_runs r JOIN monitoring.quality_results q USING (quality_run_id)
                    WHERE r.dag_id=%s AND r.task_id=%s GROUP BY r.quality_run_id""",
                    (dag.dag_id, quality_id)).fetchall()
            self.assertEqual(len(rows), 4 if target == "gold" else 1)
            for persisted in rows:
                self.assertEqual(persisted[:4], (status, severity == "CRITICAL", 1, run.run_id))
                self.assertIsNotNone(persisted[4])
                self.assertEqual(persisted[5], 1)
        if severity == "CRITICAL":
            self.assertEqual(run.state, "failed")
            self.assertEqual(states[quality_id], "failed")
            downstream = {task.task_id for task in selected.get_flat_relatives(upstream=False)}
            self.assertTrue(downstream)
            for task_id in downstream:
                self.assertEqual(states[task_id], "upstream_failed", task_id)
            for task in selected.get_flat_relatives(upstream=True):
                self.assertEqual(states[task.task_id], "success")
        else:
            self.assertEqual(run.state, "success")
            self.assertEqual(set(states.values()), {"success"})

    def test_warning_continues_at_every_quality_boundary(self):
        for target in ("silver", "gold", "warehouse"):
            with self.subTest(target=target):
                self.run_boundary(target, "WARNING", "WARN")

    def test_critical_blocks_every_downstream_task_at_every_boundary(self):
        for target in ("silver", "gold", "warehouse"):
            with self.subTest(target=target):
                self.run_boundary(target, "CRITICAL", "FAIL")

    def test_info_observation_continues(self):
        self.run_boundary("silver", "INFO", "WARN")


if __name__ == "__main__":
    unittest.main()
