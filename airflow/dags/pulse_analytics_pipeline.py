"""Orchestrate Pulse lake processing, warehouse refresh, and dbt marts."""

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
QUALITY_EXECUTION_ENV = {
    "QUALITY_ATTEMPT_NUMBER": "{{ ti.try_number }}",
    "QUALITY_MAP_INDEX": "{{ ti.map_index }}",
    "QUALITY_LOGICAL_DATE": "{{ ts }}",
}

with DAG(
    dag_id=DAG_ID,
    description="Build Pulse analytics, refresh PostgreSQL, and test dbt marts",
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
    quality_check_silver = BashOperator(
        task_id="quality_check_silver",
        bash_command="python -m src.quality.runner silver --block-on-critical --log-format jsonl --persist",
        cwd=PROJECT_ROOT,
        trigger_rule="all_success",
        do_xcom_push=False,
        env=QUALITY_EXECUTION_ENV,
        append_env=True,
    )
    build_gold = BashOperator(
        task_id="build_gold",
        bash_command="python -m src.analytics.gold_build",
        cwd=PROJECT_ROOT,
    )
    quality_check_gold = BashOperator(
        task_id="quality_check_gold",
        bash_command="python -m src.quality.runner gold --block-on-critical --log-format jsonl --persist",
        cwd=PROJECT_ROOT,
        trigger_rule="all_success",
        do_xcom_push=False,
        env=QUALITY_EXECUTION_ENV,
        append_env=True,
    )
    load_gold_to_warehouse = BashOperator(
        task_id="load_gold_to_warehouse",
        bash_command="python -m src.warehouse.load_gold load",
        cwd=PROJECT_ROOT,
    )
    quality_check_warehouse = BashOperator(
        task_id="quality_check_warehouse",
        bash_command="python -m src.quality.runner warehouse --block-on-critical --log-format jsonl --persist",
        cwd=PROJECT_ROOT,
        trigger_rule="all_success",
        do_xcom_push=False,
        env=QUALITY_EXECUTION_ENV,
        append_env=True,
    )
    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="dbt run --project-dir /opt/pulse/dbt",
        cwd=PROJECT_ROOT,
    )
    test_dbt = BashOperator(
        task_id="test_dbt",
        bash_command="dbt test --project-dir /opt/pulse/dbt",
        cwd=PROJECT_ROOT,
    )

    (
        check_bronze_available
        >> build_silver
        >> quality_check_silver
        >> build_gold
        >> quality_check_gold
        >> load_gold_to_warehouse
        >> quality_check_warehouse
        >> run_dbt
        >> test_dbt
    )
