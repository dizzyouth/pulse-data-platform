"""JSON line events shared by local and Airflow quality executions."""

from dataclasses import asdict
from datetime import datetime, timezone
import json
from uuid import UUID

from src.quality.models import summarize


def emit_event(event: str, **fields) -> None:
    print(json.dumps({"event": event, **fields},
                     default=lambda value: str(value) if isinstance(value, UUID) else value.isoformat(),
                     allow_nan=False, separators=(",", ":")), flush=True)


def log_result(result) -> None:
    emit_event("quality_result", **asdict(result))


def log_summary(results, *, target: str, completed: bool = True) -> None:
    summary = summarize(results)
    emit_event("quality_summary", target=target, checked_at_utc=datetime.now(timezone.utc),
               completed=completed, counts={"PASS": summary.passed, "WARN": summary.warnings, "FAIL": summary.failed},
               **asdict(summary))
