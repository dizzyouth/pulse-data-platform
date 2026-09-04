"""Orchestrate finite Silver and Gold processing for Pulse."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

from src.orchestration.dag_config import (
    DAG_ID,
    DAG_SCHEDULE,
    TASK_RETRIES,
    TASK_RETRY_DELAY_MINUTES,
)

PROJECT_ROOT = "/opt/pulse"

with DAG(
    dag_id=DAG_ID,
    description="Build Pulse analytics and refresh the PostgreSQL warehouse",
    schedule=DAG_SCHEDULE,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={
        "owner": "pulse-data-platform",
        "retries": TASK_RETRIES,
        "retry_delay": timedelta(minutes=TASK_RETRY_DELAY_MINUTES),
    },
    max_active_runs=1,
    tags=["pulse", "analytics"],
) as dag:
    check_bronze_available = BashOperator(
        task_id="check_bronze_available",
        bash_command="python -m src.orchestration.validation bronze",
        cwd=PROJECT_ROOT,
    )
    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=(
            "python -m src.streaming.silver_streaming --orchestrated-snapshot"
        ),
        cwd=PROJECT_ROOT,
    )
    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command="python -m src.orchestration.validation silver",
        cwd=PROJECT_ROOT,
    )
    build_gold = BashOperator(
        task_id="build_gold",
        bash_command="python -m src.analytics.gold_build",
        cwd=PROJECT_ROOT,
    )
    validate_gold = BashOperator(
        task_id="validate_gold",
        bash_command="python -m src.orchestration.validation gold",
        cwd=PROJECT_ROOT,
    )
    load_gold_to_warehouse = BashOperator(
        task_id="load_gold_to_warehouse",
        bash_command="python -m src.warehouse.load_gold load",
        cwd=PROJECT_ROOT,
    )
    validate_warehouse = BashOperator(
        task_id="validate_warehouse",
        bash_command="python -m src.warehouse.load_gold validate",
        cwd=PROJECT_ROOT,
    )

    (
        check_bronze_available
        >> build_silver
        >> validate_silver
        >> build_gold
        >> validate_gold
        >> load_gold_to_warehouse
        >> validate_warehouse
    )
