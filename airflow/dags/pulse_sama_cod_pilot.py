"""Manual, local-only Phase 6.5A real COD pilot orchestration."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = "/opt/pulse"
DBT_MODELS = " ".join(("sama_pilot_funnel", "sama_pilot_order_changes",
                       "sama_pilot_native_economics", "sama_pilot_data_quality"))

with DAG(
    dag_id="pulse_sama_real_cod_pilot",
    description="Validate, load, and publish the private Lightfunnels + COD Network pilot",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={"owner": "pulse-data-platform", "retries": 1,
                  "retry_delay": timedelta(minutes=2)},
    max_active_runs=1,
    tags=["pulse", "pilot", "sama", "cod", "manual", "private"],
) as dag:
    validate_private_sources = BashOperator(
        task_id="validate_private_sources",
        bash_command="python -m src.pilots.sama validate",
        cwd=PROJECT_ROOT,
    )
    load_sanitized_pilot = BashOperator(
        task_id="load_sanitized_pilot",
        bash_command="python -m src.warehouse.load_sama_pilot load",
        cwd=PROJECT_ROOT,
    )
    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=f"dbt run --project-dir /opt/pulse/dbt --select {DBT_MODELS}",
        cwd=PROJECT_ROOT,
    )
    test_dbt = BashOperator(
        task_id="test_dbt",
        bash_command=f"dbt test --project-dir /opt/pulse/dbt --select {DBT_MODELS}",
        cwd=PROJECT_ROOT,
    )

    validate_private_sources >> load_sanitized_pilot >> run_dbt >> test_dbt
