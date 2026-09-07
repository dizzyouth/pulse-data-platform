"""Tests for the Airflow DAG contract and dataset validation tasks."""

from __future__ import annotations

import importlib.util
import os
import sys
import types
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from pyspark.sql import SparkSession

from src.analytics.gold_build import (
    GoldPaths,
    SILVER_VALID_SCHEMA,
    build_gold_tables,
    write_gold_tables,
)
from src.orchestration.dag_config import (
    DAG_ID,
    DAG_SCHEDULE,
    TASK_DEPENDENCIES,
    TASK_IDS,
    TASK_RETRIES,
)
from src.orchestration.validation import (
    validate_bronze_available,
    validate_gold_output,
    validate_silver_output,
)
from src.streaming.silver_streaming import BRONZE_VALID_SCHEMA


class FakeDag:
    active: "FakeDag | None" = None

    def __init__(self, **kwargs) -> None:
        self.dag_id = kwargs["dag_id"]
        self.schedule = kwargs["schedule"]
        self.default_args = kwargs["default_args"]
        self.task_dict = {}

    def __enter__(self):
        FakeDag.active = self
        return self

    def __exit__(self, *_args) -> None:
        FakeDag.active = None


class FakeBashOperator:
    def __init__(self, *, task_id: str, **_kwargs) -> None:
        self.task_id = task_id
        self.kwargs = _kwargs
        self.downstream_task_ids: set[str] = set()
        if FakeDag.active is not None:
            FakeDag.active.task_dict[task_id] = self

    def __rshift__(self, other):
        self.downstream_task_ids.add(other.task_id)
        return other


class AirflowDagContractTests(unittest.TestCase):
    def test_dag_imports_with_expected_manual_task_graph(self) -> None:
        airflow = types.ModuleType("airflow")
        airflow.DAG = FakeDag
        operators = types.ModuleType("airflow.operators")
        bash = types.ModuleType("airflow.operators.bash")
        bash.BashOperator = FakeBashOperator
        dag_path = (
            Path(__file__).resolve().parents[1]
            / "airflow"
            / "dags"
            / "pulse_analytics_pipeline.py"
        )
        spec = importlib.util.spec_from_file_location("pulse_test_dag", dag_path)
        module = importlib.util.module_from_spec(spec)

        with patch.dict(
            sys.modules,
            {
                "airflow": airflow,
                "airflow.operators": operators,
                "airflow.operators.bash": bash,
            },
        ):
            assert spec.loader is not None
            spec.loader.exec_module(module)

        self.assertEqual(module.dag.dag_id, DAG_ID)
        self.assertIs(module.dag.schedule, DAG_SCHEDULE)
        self.assertEqual(module.dag.default_args["retries"], TASK_RETRIES)
        self.assertEqual(set(module.dag.task_dict), set(TASK_IDS))
        actual_dependencies = {
            (task_id, downstream)
            for task_id, task in module.dag.task_dict.items()
            for downstream in task.downstream_task_ids
        }
        self.assertEqual(actual_dependencies, set(TASK_DEPENDENCIES))
        self.assertGreaterEqual(TASK_RETRIES, 0)
        self.assertLessEqual(TASK_RETRIES, 2)
        self.assertEqual(TASK_IDS, (
            "check_bronze_available", "build_silver", "quality_check_silver",
            "build_gold", "quality_check_gold", "load_gold_to_warehouse",
            "quality_check_warehouse", "anomaly_check", "run_dbt", "test_dbt",
        ))
        for target in ("silver", "gold", "warehouse"):
            task = module.dag.task_dict[f"quality_check_{target}"]
            self.assertEqual(task.kwargs["bash_command"],
                             f"python -m src.quality.runner {target} --block-on-critical --log-format jsonl --persist")
            self.assertEqual(task.kwargs["cwd"], "/opt/pulse")
            self.assertEqual(task.kwargs["trigger_rule"], "all_success")
            self.assertFalse(task.kwargs["do_xcom_push"])
            self.assertTrue(task.kwargs["append_env"])
            self.assertEqual(task.kwargs["env"], {
                "QUALITY_ATTEMPT_NUMBER": "{{ ti.try_number }}",
                "QUALITY_MAP_INDEX": "{{ ti.map_index }}",
                "QUALITY_LOGICAL_DATE": "{{ ts }}",
            })
        anomaly = module.dag.task_dict["anomaly_check"]
        self.assertEqual(anomaly.kwargs["bash_command"],
                         "python -m src.quality.anomaly_runner --persist --log-format jsonl")
        self.assertEqual(anomaly.kwargs["trigger_rule"], "all_success")
        self.assertNotIn("--block-on-critical", anomaly.kwargs["bash_command"])


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS", "1") == "1", "Spark tests disabled")
class OrchestrationValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("pulse-orchestration-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    @staticmethod
    def bronze_row(event_id: str = "evt_1") -> dict:
        timestamp = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
        return {
            "event_id": event_id,
            "event_type": "payment_completed",
            "event_timestamp": timestamp,
            "customer_id": "cus_1",
            "session_id": "ses_1",
            "country": "US",
            "product_id": "prd_1",
            "seller_id": "sel_1",
            "order_id": "ord_1",
            "payment_id": "pay_1",
            "quantity": 2,
            "unit_price": 10.0,
            "currency": "USD",
            "kafka_key": "cus_1",
            "kafka_topic": "marketplace.events",
            "kafka_partition": 0,
            "kafka_offset": 1,
            "kafka_timestamp": timestamp,
            "raw_json": "{}",
            "validation_errors": [],
            "ingested_at_utc": timestamp,
            "ingestion_date": date(2026, 1, 1),
        }

    @classmethod
    def silver_row(cls, event_id: str = "evt_1") -> dict:
        row = cls.bronze_row(event_id)
        row.pop("raw_json")
        row.pop("validation_errors")
        row.pop("ingestion_date")
        row["event_date"] = date(2026, 1, 1)
        return row

    def test_bronze_validation_passes_and_missing_path_fails(self) -> None:
        with TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            bronze_path = Path(directory, "bronze")
            self.spark.createDataFrame(
                [self.bronze_row()], BRONZE_VALID_SCHEMA
            ).write.parquet(str(bronze_path))
            metadata = bronze_path / "_spark_metadata"
            metadata.mkdir()
            (metadata / "0").write_text(
                'v1\n{"path":"file:///C:/unmounted/bronze.parquet"}\n',
                encoding="utf-8",
            )

            self.assertEqual(validate_bronze_available(self.spark, bronze_path), 1)
            with self.assertRaises(FileNotFoundError):
                validate_bronze_available(self.spark, Path(directory, "missing"))

    def test_silver_validation_passes_and_duplicate_ids_fail(self) -> None:
        with TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            root = Path(directory)
            valid_path = root / "valid"
            rejected_path = root / "rejected"
            self.spark.createDataFrame(
                [self.silver_row()], SILVER_VALID_SCHEMA
            ).write.parquet(str(valid_path))

            result = validate_silver_output(self.spark, valid_path, rejected_path)
            self.assertEqual(result, {"valid": 1, "rejected": None})

            duplicate_path = root / "duplicate"
            self.spark.createDataFrame(
                [self.silver_row(), self.silver_row()], SILVER_VALID_SCHEMA
            ).write.parquet(str(duplicate_path))
            with self.assertRaisesRegex(ValueError, "duplicate event_id"):
                validate_silver_output(self.spark, duplicate_path, rejected_path)

    def test_gold_validation_passes_and_missing_table_fails(self) -> None:
        with TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            root = Path(directory)
            paths = GoldPaths(
                silver_source=root / "silver",
                daily_sales=root / "gold" / "daily_sales",
                customer_metrics=root / "gold" / "customer_metrics",
                product_metrics=root / "gold" / "product_metrics",
                funnel_metrics=root / "gold" / "funnel_metrics",
            )
            silver = self.spark.createDataFrame(
                [self.silver_row()], SILVER_VALID_SCHEMA
            )
            write_gold_tables(build_gold_tables(silver), paths)

            counts = validate_gold_output(self.spark, paths)
            self.assertEqual(
                counts,
                {
                    "daily_sales": 1,
                    "customer_metrics": 1,
                    "product_metrics": 1,
                    "funnel_metrics": 1,
                },
            )

            missing_paths = GoldPaths(
                silver_source=paths.silver_source,
                daily_sales=root / "missing",
                customer_metrics=paths.customer_metrics,
                product_metrics=paths.product_metrics,
                funnel_metrics=paths.funnel_metrics,
            )
            with self.assertRaises(FileNotFoundError):
                validate_gold_output(self.spark, missing_paths)


if __name__ == "__main__":
    unittest.main()
