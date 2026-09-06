"""Airflow-independent contract for the Pulse analytics DAG."""

from __future__ import annotations

DAG_ID = "pulse_analytics_pipeline"
DAG_SCHEDULE = None
TASK_RETRIES = 1
TASK_RETRY_DELAY_MINUTES = 1
TASK_IDS = (
    "check_bronze_available",
    "build_silver",
    "quality_check_silver",
    "build_gold",
    "quality_check_gold",
    "load_gold_to_warehouse",
    "quality_check_warehouse",
    "run_dbt",
    "test_dbt",
)
TASK_DEPENDENCIES = tuple(zip(TASK_IDS, TASK_IDS[1:]))
