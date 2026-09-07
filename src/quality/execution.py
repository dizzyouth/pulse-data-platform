"""Database-independent execution envelopes and retry identity."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from uuid import UUID, NAMESPACE_URL, uuid4, uuid5

from src.quality.models import QualityResult


@dataclass(frozen=True, slots=True, kw_only=True)
class ExecutionContext:
    execution_source: str = "cli"
    execution_id: str
    dag_id: str | None = None
    airflow_run_id: str | None = None
    task_id: str | None = None
    attempt_number: int = 1
    map_index: int = -1
    logical_date_utc: datetime | None = None

    def __post_init__(self):
        if not self.execution_source.strip() or not self.execution_id.strip():
            raise ValueError("Execution source and ID are required")
        if type(self.attempt_number) is not int or self.attempt_number < 1:
            raise ValueError("attempt_number must be a positive integer")
        if type(self.map_index) is not int or self.map_index < -1:
            raise ValueError("map_index must be -1 or nonnegative")
        if self.execution_source == "airflow" and not all((self.dag_id, self.airflow_run_id, self.task_id)):
            raise ValueError("Airflow persistence requires DAG, run, and task IDs")
        if self.logical_date_utc is not None and self.logical_date_utc.utcoffset() is None:
            raise ValueError("logical_date_utc must be timezone-aware")

    def run_id(self, dataset_name: str, layer: str) -> UUID:
        key = ["pulse-quality-v1", self.execution_source, self.execution_id, self.dag_id,
               self.airflow_run_id, self.task_id, self.map_index, self.attempt_number, dataset_name, layer]
        return uuid5(NAMESPACE_URL, json.dumps(key, separators=(",", ":")))

    def logical_id(self, namespace: str, *parts) -> UUID:
        """Stable across retries for idempotent derived events."""
        key = [namespace, self.execution_source, self.execution_id, self.dag_id,
               self.airflow_run_id, self.task_id, self.map_index, *parts]
        return uuid5(NAMESPACE_URL, json.dumps(key, separators=(",", ":"), sort_keys=True))


def execution_context(*, execution_id: str | None = None, attempt_number: int | None = None,
                      environ: Mapping[str, str] | None = None) -> ExecutionContext:
    env = os.environ if environ is None else environ
    dag, run, task = (env.get(name) for name in
                      ("AIRFLOW_CTX_DAG_ID", "AIRFLOW_CTX_DAG_RUN_ID", "AIRFLOW_CTX_TASK_ID"))
    if any((dag, run, task)):
        if execution_id is not None or attempt_number is not None:
            raise ValueError("Airflow execution identity cannot be overridden by CLI options")
        attempt = env.get("QUALITY_ATTEMPT_NUMBER")
        if not all((dag, run, task, attempt)):
            raise ValueError("Incomplete Airflow persistence context, including attempt number")
        logical = env.get("QUALITY_LOGICAL_DATE") or env.get("AIRFLOW_CTX_EXECUTION_DATE")
        return ExecutionContext(execution_source="airflow", execution_id=run, dag_id=dag,
                                airflow_run_id=run, task_id=task, attempt_number=int(attempt),
                                map_index=int(env.get("QUALITY_MAP_INDEX", "-1")),
                                logical_date_utc=datetime.fromisoformat(logical) if logical else None)
    return ExecutionContext(execution_id=str(uuid4()) if execution_id is None else execution_id,
                            attempt_number=1 if attempt_number is None else attempt_number)


@dataclass(frozen=True, slots=True, kw_only=True)
class DatasetQualityRun:
    dataset_name: str
    layer: str
    started_at_utc: datetime
    completed_at_utc: datetime
    results: tuple[QualityResult, ...]

    def __post_init__(self):
        if not self.dataset_name.strip() or not self.layer.strip():
            raise ValueError("Dataset and layer are required")
        for name in ("started_at_utc", "completed_at_utc"):
            value = getattr(self, name)
            if value.utcoffset() is None:
                raise ValueError("Execution timestamps must be timezone-aware")
            object.__setattr__(self, name, value.astimezone(timezone.utc))
        if self.completed_at_utc < self.started_at_utc:
            raise ValueError("Completion precedes execution start")
        object.__setattr__(self, "results", tuple(self.results))
        if any((r.dataset_name, r.layer) != (self.dataset_name, self.layer) for r in self.results):
            raise ValueError("A quality run must contain only one dataset/layer")
        if len({r.check_name for r in self.results}) != len(self.results):
            raise ValueError("Duplicate check names within a quality run")
