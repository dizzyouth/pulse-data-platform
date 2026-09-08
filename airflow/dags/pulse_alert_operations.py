"""Deliver and escalate alerts independently from the analytics pipeline."""

from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

from src.orchestration.alert_operations_config import (
    ALERT_OPERATIONS_DAG_ID,
    ALERT_OPERATIONS_SCHEDULE,
    ALERT_OPERATIONS_TASK_ID,
)


with DAG(
    dag_id=ALERT_OPERATIONS_DAG_ID,
    description="Deliver due Pulse alerts without blocking analytics processing",
    schedule=ALERT_OPERATIONS_SCHEDULE,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={"owner": "pulse-data-platform", "retries": 0},
    max_active_runs=1,
    tags=["pulse", "monitoring", "alerts"],
) as dag:
    alert_delivery_sweep = BashOperator(
        task_id=ALERT_OPERATIONS_TASK_ID,
        bash_command="python -m src.quality.delivery_cli sweep --limit 100",
        cwd="/opt/pulse",
        do_xcom_push=False,
    )
