"""Manual, local-only Phase 6.4B UCI Online Retail II benchmark orchestration."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = "/opt/pulse"
DBT_UCI_MODELS = " ".join((
    "uci_retail_daily", "uci_invoice_summary", "uci_line_classification",
    "uci_country_distribution", "uci_data_quality", "uci_economic_completeness",
))

with DAG(
    dag_id="pulse_uci_online_retail_ii_benchmark",
    description="Validate, build, load, and test the local UCI retail portability benchmark",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={"owner": "pulse-data-platform", "retries": 1,
                  "retry_delay": timedelta(minutes=2)},
    max_active_runs=1,
    tags=["pulse", "benchmark", "uci", "retail", "manual"],
) as dag:
    validate_local_files = BashOperator(
        task_id="validate_local_files",
        bash_command="python -m src.benchmarks.uci_online_retail_ii validate",
        cwd=PROJECT_ROOT,
    )
    run_benchmark = BashOperator(
        task_id="run_benchmark",
        bash_command=("python -m src.benchmarks.uci_online_retail_ii benchmark "
                      "--enforce-acceptance"),
        cwd=PROJECT_ROOT,
    )
    load_benchmark = BashOperator(
        task_id="load_benchmark",
        bash_command="python -m src.warehouse.load_uci_online_retail_ii load",
        cwd=PROJECT_ROOT,
    )
    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=f"dbt run --project-dir /opt/pulse/dbt --select {DBT_UCI_MODELS}",
        cwd=PROJECT_ROOT,
    )
    test_dbt = BashOperator(
        task_id="test_dbt",
        bash_command=f"dbt test --project-dir /opt/pulse/dbt --select {DBT_UCI_MODELS}",
        cwd=PROJECT_ROOT,
    )

    validate_local_files >> run_benchmark >> load_benchmark >> run_dbt >> test_dbt
