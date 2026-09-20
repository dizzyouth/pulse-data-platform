"""Manual, local-only Phase 6.4A Olist benchmark orchestration."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = "/opt/pulse"
DBT_OLIST_MODELS = " ".join((
    "olist_orders_by_status", "olist_commerce_daily", "olist_payment_methods",
    "olist_data_quality", "olist_economic_completeness",
))

with DAG(
    dag_id="pulse_olist_public_benchmark",
    description="Validate, build, load, and test the local Olist public benchmark",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={"owner": "pulse-data-platform", "retries": 1,
                  "retry_delay": timedelta(minutes=2)},
    max_active_runs=1,
    tags=["pulse", "benchmark", "olist", "manual"],
) as dag:
    validate_local_files = BashOperator(
        task_id="validate_local_files",
        bash_command="python -m src.benchmarks.olist validate",
        cwd=PROJECT_ROOT,
    )
    run_benchmark = BashOperator(
        task_id="run_benchmark",
        bash_command="python -m src.benchmarks.olist benchmark --enforce-acceptance",
        cwd=PROJECT_ROOT,
    )
    load_benchmark = BashOperator(
        task_id="load_benchmark",
        bash_command="python -m src.warehouse.load_olist_benchmark load",
        cwd=PROJECT_ROOT,
    )
    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=f"dbt run --project-dir /opt/pulse/dbt --select {DBT_OLIST_MODELS}",
        cwd=PROJECT_ROOT,
    )
    test_dbt = BashOperator(
        task_id="test_dbt",
        bash_command=f"dbt test --project-dir /opt/pulse/dbt --select {DBT_OLIST_MODELS}",
        cwd=PROJECT_ROOT,
    )

    validate_local_files >> run_benchmark >> load_benchmark >> run_dbt >> test_dbt
