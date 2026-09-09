"""Validate registered businesses and exercise enabled local adapters."""

from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

from src.orchestration.business_sources import ONBOARDING_DAG_ID, discover_enabled_sources


PROJECT_ROOT = "/opt/pulse"
SOURCES = discover_enabled_sources()

with DAG(
    dag_id=ONBOARDING_DAG_ID,
    description="Validate onboarding contracts and local source adapters without external API calls",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={"owner": "pulse-data-platform", "retries": 0},
    max_active_runs=1,
    tags=["pulse", "onboarding", "sources"],
) as dag:
    validate_registry = BashOperator(
        task_id="validate_business_registry",
        bash_command="python -m src.onboarding.cli validate-all",
        cwd=PROJECT_ROOT,
        do_xcom_push=False,
    )
    for source in SOURCES:
        source_check = BashOperator(
            task_id=source.task_id,
            bash_command=(
                "python -m src.onboarding.cli source-check "
                f"{source.business_id} {source.source_id}"
            ),
            cwd=PROJECT_ROOT,
            do_xcom_push=False,
        )
        validate_registry >> source_check
