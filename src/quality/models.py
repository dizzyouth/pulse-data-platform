"""Typed rules, results, and run summaries. Ratios use fractions, not percent."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
import json
import math
from typing import Iterable


class Status(StrEnum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


class Severity(StrEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True, slots=True, kw_only=True)
class QualityContext:
    dataset_name: str
    layer: str
    checked_at_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    reference_count: int | None = None

    def __post_init__(self) -> None:
        if not self.dataset_name.strip() or not self.layer.strip():
            raise ValueError("dataset_name and layer are required")
        if self.checked_at_utc.utcoffset() is None:
            raise ValueError("checked_at_utc must be timezone-aware")
        object.__setattr__(self, "checked_at_utc", self.checked_at_utc.astimezone(timezone.utc))
        if self.reference_count is not None and (
            type(self.reference_count) is not int or self.reference_count < 0
        ):
            raise ValueError("reference_count must be a nonnegative integer")


@dataclass(frozen=True, slots=True, kw_only=True)
class Rule:
    check_name: str
    severity: Severity = Severity.CRITICAL


@dataclass(frozen=True, slots=True, kw_only=True)
class RowCount(Rule):
    min_rows: int = 0


@dataclass(frozen=True, slots=True, kw_only=True)
class NullRatio(Rule):
    column: str
    max_ratio: float = 0.0


@dataclass(frozen=True, slots=True, kw_only=True)
class Uniqueness(Rule):
    columns: tuple[str, ...]
    max_duplicate_ratio: float = 0.0


@dataclass(frozen=True, slots=True, kw_only=True)
class AllowedValues(Rule):
    column: str
    values: tuple[str | int | float, ...]
    allow_null: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class NumericBounds(Rule):
    column: str
    minimum: float | None = None
    maximum: float | None = None
    minimum_inclusive: bool = True
    allow_null: bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class Pattern(Rule):
    column: str
    pattern: str
    allow_null: bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class Freshness(Rule):
    column: str
    max_age_seconds: float
    max_future_seconds: float = 0.0


@dataclass(frozen=True, slots=True, kw_only=True)
class VolumeChange(Rule):
    max_deviation_ratio: float


@dataclass(frozen=True, slots=True, kw_only=True)
class CheckDetails:
    message: str = ""
    row_count: int | None = None
    violating_rows: int | None = None
    duplicate_count: int | None = None
    duplicate_rate: float | None = None
    latest_timestamp_utc: datetime | None = None
    reference_count: int | None = None
    deduplicated_rows: int | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class QualityResult:
    check_name: str
    dataset_name: str
    layer: str
    status: Status
    severity: Severity
    metric_name: str
    observed_value: int | float | None
    expected_value: int | float | str
    checked_at_utc: datetime
    details: CheckDetails = field(default_factory=CheckDetails)

    def __post_init__(self) -> None:
        if not isinstance(self.status, Status) or not isinstance(self.severity, Severity):
            raise ValueError("Results require explicit Status and Severity values")
        if self.checked_at_utc.utcoffset() is None:
            raise ValueError("Result timestamps must be timezone-aware")
        object.__setattr__(self, "checked_at_utc", self.checked_at_utc.astimezone(timezone.utc))
        if any(not isinstance(name, str) or not name.strip() for name in
               (self.check_name, self.dataset_name, self.layer, self.metric_name)):
            raise ValueError("Result identity and metric names are required")
        if isinstance(self.observed_value, float) and not math.isfinite(self.observed_value):
            raise ValueError("Undefined observations must use None, not NaN/infinity")


@dataclass(frozen=True, slots=True)
class QualitySummary:
    total_checks: int
    passed: int
    warnings: int
    failed: int
    critical_failures: int
    overall_status: Status


def summarize(results: Iterable[QualityResult]) -> QualitySummary:
    results = tuple(results)
    passed = sum(r.status == Status.PASS for r in results)
    warnings = sum(r.status == Status.WARN for r in results)
    failed = sum(r.status == Status.FAIL for r in results)
    critical = sum(r.status == Status.FAIL and r.severity == Severity.CRITICAL for r in results)
    overall = Status.FAIL if critical else Status.WARN if warnings or failed else Status.PASS
    return QualitySummary(len(results), passed, warnings, failed, critical, overall)


def should_block(results: Iterable[QualityResult]) -> bool:
    return summarize(results).critical_failures > 0


def report_json(results: Iterable[QualityResult]) -> str:
    results = tuple(results)
    return json.dumps(
        {"summary": asdict(summarize(results)), "results": [asdict(r) for r in results]},
        default=lambda value: value.isoformat(), allow_nan=False, indent=2,
    )
