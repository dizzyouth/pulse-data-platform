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
        dag.dag_id = f"phase52_{target}_{severity.lower()}"
        quality_id = f"quality_check_{target}"
        selected = dag.get_task(quality_id)
        args = shlex.split(selected.bash_command)[3:]
        code = textwrap.dedent(f"""
            from unittest.mock import patch
            from datetime import datetime, timezone
            from src.quality.models import QualityResult, Severity, Status
            from src.quality.runner import main
            result = QualityResult(check_name='boundary_fixture', dataset_name='fixture',
                layer={target!r}, status=Status({status!r}), severity=Severity({severity!r}),
                metric_name='row_count', observed_value=0, expected_value=1,
                checked_at_utc=datetime.now(timezone.utc))
            with patch('src.analytics.gold_build.build_gold_spark_session'), \\
                 patch('src.quality.runner.iter_target_results', return_value=[result]):
                raise SystemExit(main({args!r}))
        """)
        for task in dag.tasks:
            self.assertEqual(task.trigger_rule, "all_success")
            task.retries = 0  # Exercise terminal states without a retry delay.
            task.bash_command = "true"
        selected.bash_command = "python -c " + shlex.quote(code)
        run = dag.test(execution_date=timezone.datetime(2026, 1, 2))
        states = {ti.task_id: ti.state for ti in run.get_task_instances()}
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
