"""Database-independent, deterministic anomaly evaluation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
import math
from statistics import median
from uuid import UUID

from src.quality.models import Severity


class AnomalyStatus(StrEnum):
    NORMAL = "NORMAL"
    ANOMALY = "ANOMALY"
    INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"


@dataclass(frozen=True, slots=True, kw_only=True)
class AnomalyPolicy:
    minimum_history: int = 7
    warning_z: float = 3.5
    critical_z: float = 6.0
    warning_ratio: float = 0.5
    critical_ratio: float = 1.0
    warning_absolute: float = 1.0
    critical_absolute: float = 3.0

    def __post_init__(self):
        if self.minimum_history < 2:
            raise ValueError("minimum_history must be at least two")
        for warning, critical in ((self.warning_z, self.critical_z),
                                  (self.warning_ratio, self.critical_ratio),
                                  (self.warning_absolute, self.critical_absolute)):
            if not (math.isfinite(warning) and math.isfinite(critical) and 0 < warning <= critical):
                raise ValueError("Anomaly thresholds must be positive, finite, and ordered")


@dataclass(frozen=True, slots=True, kw_only=True)
class MetricSeries:
    metric_name: str
    dataset_name: str
    layer: str
    current_value: float
    history: tuple[float, ...]
    observed_at_utc: datetime
    dimensions: dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if not all(value.strip() for value in (self.metric_name, self.dataset_name, self.layer)):
            raise ValueError("Metric identity is required")
        values = (self.current_value, *self.history)
        if any(isinstance(value, bool) or not math.isfinite(value) for value in values):
            raise ValueError("Anomaly observations must be finite numbers")
        if self.observed_at_utc.utcoffset() is None:
            raise ValueError("observed_at_utc must be timezone-aware")
        object.__setattr__(self, "observed_at_utc", self.observed_at_utc.astimezone(timezone.utc))
        object.__setattr__(self, "history", tuple(float(value) for value in self.history))
        object.__setattr__(self, "current_value", float(self.current_value))


@dataclass(frozen=True, slots=True, kw_only=True)
class AnomalyResult:
    anomaly_id: UUID
    metric_name: str
    dataset_name: str
    layer: str
    current_value: float
    baseline_value: float | None
    deviation_value: float | None
    deviation_percent: float | None
    threshold: dict[str, float | int | str]
    method: str
    status: AnomalyStatus
    severity: Severity
    observed_at_utc: datetime
    explanation: str
    history_count: int
    dimensions: dict[str, str] = field(default_factory=dict)
    details: dict = field(default_factory=dict)


def evaluate(series: MetricSeries, policy: AnomalyPolicy, anomaly_id: UUID) -> AnomalyResult:
    """Use median/MAD; flat baselines fall back to relative or absolute change."""
    count = len(series.history)
    threshold = {"minimum_history": policy.minimum_history, "warning_z": policy.warning_z,
                 "critical_z": policy.critical_z, "warning_ratio": policy.warning_ratio,
                 "critical_ratio": policy.critical_ratio, "warning_absolute": policy.warning_absolute,
                 "critical_absolute": policy.critical_absolute, "boundary": "inclusive"}
    common = dict(anomaly_id=anomaly_id, metric_name=series.metric_name,
                  dataset_name=series.dataset_name, layer=series.layer,
                  current_value=series.current_value, threshold=threshold,
                  observed_at_utc=series.observed_at_utc, history_count=count,
                  dimensions=series.dimensions)
    if count < policy.minimum_history:
        return AnomalyResult(**common, baseline_value=None, deviation_value=None,
                             deviation_percent=None, method="median_mad",
                             status=AnomalyStatus.INSUFFICIENT_HISTORY, severity=Severity.INFO,
                             explanation=f"Need {policy.minimum_history} prior observations; found {count}.")
    baseline = float(median(series.history))
    deviation = series.current_value - baseline
    deviation_percent = None if baseline == 0 else deviation / abs(baseline) * 100
    mad = float(median(abs(value - baseline) for value in series.history))
    if mad > 0:
        method, score = "modified_z_score", abs(0.67448975 * deviation / mad)
        warning, critical, units = policy.warning_z, policy.critical_z, "modified z-score"
    elif baseline != 0:
        method, score = "percentage_deviation", abs(deviation / baseline)
        warning, critical, units = policy.warning_ratio, policy.critical_ratio, "ratio"
    else:
        method, score = "absolute_deviation", abs(deviation)
        warning, critical, units = policy.warning_absolute, policy.critical_absolute, "units"
    anomalous = score >= warning
    severity = Severity.CRITICAL if score >= critical else Severity.WARNING if anomalous else Severity.INFO
    status = AnomalyStatus.ANOMALY if anomalous else AnomalyStatus.NORMAL
    explanation = (f"Current {series.current_value:g}; median baseline {baseline:g}; deviation {deviation:g} "
                   f"({deviation_percent:.2f}%); {method} {score:.4g} {units}; "
                   f"warning threshold {warning:g}, critical threshold {critical:g}.") if deviation_percent is not None else (
                   f"Current {series.current_value:g}; median baseline {baseline:g}; deviation {deviation:g}; "
                   f"{method} {score:.4g} {units}; warning threshold {warning:g}, critical threshold {critical:g}.")
    return AnomalyResult(**common, baseline_value=baseline, deviation_value=deviation,
                         deviation_percent=deviation_percent, method=method, status=status,
                         severity=severity, explanation=explanation,
                         details={"median_absolute_deviation": mad, "score": score})
