"""Database-independent, deterministic anomaly evaluation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
import math
from statistics import median
from uuid import UUID

from src.quality.baselines import STRATEGIES, BaselineEstimate
from src.quality.models import Severity


class AnomalyStatus(StrEnum):
    NORMAL = "NORMAL"
    ANOMALY = "ANOMALY"
    INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"


class BaselineConfidence(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


@dataclass(frozen=True, slots=True, kw_only=True)
class AnomalyPolicy:
    minimum_history: int = 7
    warning_z: float = 3.5
    critical_z: float = 6.0
    warning_ratio: float = 0.5
    critical_ratio: float = 1.0
    warning_absolute: float = 1.0
    critical_absolute: float = 3.0
    baseline_strategies: tuple[str, ...] = ("robust_history",)
    minimum_same_weekday_history: int = 4
    minimum_trend_history: int = 7
    minimum_seasonal_trend_history: int = 28
    maximum_training_window: int = 56
    minimum_absolute_deviation: float = 0.0
    minimum_sample_size: int = 0

    def __post_init__(self):
        if self.minimum_history < 2:
            raise ValueError("minimum_history must be at least two")
        for warning, critical in ((self.warning_z, self.critical_z),
                                  (self.warning_ratio, self.critical_ratio),
                                  (self.warning_absolute, self.critical_absolute)):
            if not (math.isfinite(warning) and math.isfinite(critical) and 0 < warning <= critical):
                raise ValueError("Anomaly thresholds must be positive, finite, and ordered")
        if not self.baseline_strategies or any(name not in STRATEGIES for name in self.baseline_strategies):
            raise ValueError("baseline_strategies must contain known strategy names")
        if min(self.minimum_same_weekday_history, self.minimum_trend_history,
               self.minimum_seasonal_trend_history, self.maximum_training_window) < 2:
            raise ValueError("Contextual history requirements must be at least two")
        if self.maximum_training_window < max(self.minimum_same_weekday_history,
                                               self.minimum_trend_history):
            raise ValueError("maximum_training_window is smaller than a required history")
        if self.minimum_absolute_deviation < 0 or self.minimum_sample_size < 0:
            raise ValueError("Minimum deviation and sample size cannot be negative")


@dataclass(frozen=True, slots=True, kw_only=True)
class MetricSeries:
    metric_name: str
    dataset_name: str
    layer: str
    current_value: float
    history: tuple[float, ...]
    observed_at_utc: datetime
    dimensions: dict[str, str] = field(default_factory=dict)
    history_observed_at_utc: tuple[datetime, ...] = ()
    current_sample_size: int | None = None
    history_sample_sizes: tuple[int | None, ...] = ()

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
        if self.history_observed_at_utc and len(self.history_observed_at_utc) != len(self.history):
            raise ValueError("History timestamps must align with history values")
        if any(stamp.utcoffset() is None for stamp in self.history_observed_at_utc):
            raise ValueError("History timestamps must be timezone-aware")
        timestamps = tuple(stamp.astimezone(timezone.utc) for stamp in self.history_observed_at_utc)
        if any(left > right for left, right in zip(timestamps, timestamps[1:])):
            raise ValueError("History timestamps must be ordered")
        if any(stamp > self.observed_at_utc for stamp in timestamps):
            raise ValueError("History cannot contain future observations")
        object.__setattr__(self, "history_observed_at_utc", timestamps)
        if self.history_sample_sizes and len(self.history_sample_sizes) != len(self.history):
            raise ValueError("History sample sizes must align with history values")
        sample_sizes = (self.current_sample_size, *self.history_sample_sizes)
        if any(value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0)
               for value in sample_sizes):
            raise ValueError("Sample sizes must be nonnegative integers")


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
    baseline_strategy: str | None = None
    expected_value: float | None = None
    lower_bound: float | None = None
    upper_bound: float | None = None
    trend_slope: float | None = None
    seasonal_reference_count: int = 0
    training_window_size: int = 0
    model_error: float | None = None
    residual: float | None = None
    fallback_used: bool = False
    confidence: BaselineConfidence = BaselineConfidence.LOW


def _confidence(estimate: BaselineEstimate, policy: AnomalyPolicy, fallback_used: bool):
    scale = abs(estimate.expected_value)
    relative_error = estimate.model_error / scale if scale > 1e-12 else estimate.model_error
    if estimate.strategy in ("day_of_week", "seasonal_trend"):
        strong_count = estimate.seasonal_reference_count >= 8
    else:
        strong_count = estimate.training_window_size >= max(14, policy.minimum_history * 2)
    if strong_count and relative_error <= .15:
        level = BaselineConfidence.HIGH
    elif relative_error <= .35:
        level = BaselineConfidence.MEDIUM
    else:
        level = BaselineConfidence.LOW
    if fallback_used and level == BaselineConfidence.HIGH:
        return BaselineConfidence.MEDIUM
    return level


def _select_baseline(series: MetricSeries, policy: AnomalyPolicy):
    attempted = []
    for index, name in enumerate(policy.baseline_strategies):
        attempted.append(name)
        estimate = STRATEGIES[name].estimate(series, policy)
        if estimate is not None:
            return estimate, index > 0, tuple(attempted)
    return None, False, tuple(attempted)


def evaluate(series: MetricSeries, policy: AnomalyPolicy, anomaly_id: UUID) -> AnomalyResult:
    """Evaluate the first qualifying configured baseline without future observations."""
    raw_history_count = len(series.history)
    baseline_series = series
    if policy.minimum_sample_size > 0 and series.history_sample_sizes:
        eligible = tuple(index for index, sample in enumerate(series.history_sample_sizes)
                         if sample is not None and sample >= policy.minimum_sample_size)
        baseline_series = MetricSeries(
            metric_name=series.metric_name, dataset_name=series.dataset_name, layer=series.layer,
            dimensions=series.dimensions, current_value=series.current_value,
            observed_at_utc=series.observed_at_utc, current_sample_size=series.current_sample_size,
            history=tuple(series.history[index] for index in eligible),
            history_observed_at_utc=tuple(series.history_observed_at_utc[index] for index in eligible)
            if series.history_observed_at_utc else (),
            history_sample_sizes=tuple(series.history_sample_sizes[index] for index in eligible))
    count = len(baseline_series.history)
    threshold = {"minimum_history": policy.minimum_history, "warning_z": policy.warning_z,
                 "critical_z": policy.critical_z, "warning_ratio": policy.warning_ratio,
                 "critical_ratio": policy.critical_ratio, "warning_absolute": policy.warning_absolute,
                 "critical_absolute": policy.critical_absolute,
                 "minimum_same_weekday_history": policy.minimum_same_weekday_history,
                 "minimum_trend_history": policy.minimum_trend_history,
                 "minimum_seasonal_trend_history": policy.minimum_seasonal_trend_history,
                 "maximum_training_window": policy.maximum_training_window,
                 "minimum_absolute_deviation": policy.minimum_absolute_deviation,
                 "minimum_sample_size": policy.minimum_sample_size,
                 "boundary": "inclusive"}
    common = dict(anomaly_id=anomaly_id, metric_name=series.metric_name,
                  dataset_name=series.dataset_name, layer=series.layer,
                  current_value=series.current_value, threshold=threshold,
                  observed_at_utc=series.observed_at_utc, history_count=count,
                  dimensions=series.dimensions)
    sample_too_small = (policy.minimum_sample_size > 0 and
                        (series.current_sample_size is None or
                         series.current_sample_size < policy.minimum_sample_size))
    estimate, fallback_used, attempted = _select_baseline(baseline_series, policy)
    if sample_too_small or estimate is None:
        reason = (f"Need current sample size {policy.minimum_sample_size}; found "
                  f"{series.current_sample_size if series.current_sample_size is not None else 'unknown'}.") if sample_too_small else (
                  f"No configured baseline qualified from {', '.join(attempted)}; "
                  f"found {count} prior observations.")
        return AnomalyResult(**common, baseline_value=None, deviation_value=None,
                             deviation_percent=None, method="median_mad",
                             status=AnomalyStatus.INSUFFICIENT_HISTORY, severity=Severity.INFO,
                             explanation=(f"Need {policy.minimum_history} prior observations; found {count}."
                                          if not sample_too_small and policy.baseline_strategies == ("robust_history",)
                                          else reason),
                             details={"requested_baseline_strategy": policy.baseline_strategies[0],
                                      "attempted_baseline_strategies": list(attempted),
                                      "raw_history_count": raw_history_count,
                                      "insufficient_reason": "minimum_sample_size" if sample_too_small else "baseline_history"})
    baseline = estimate.expected_value
    deviation = series.current_value - baseline
    deviation_percent = None if baseline == 0 else deviation / abs(baseline) * 100
    residual_center = float(median(estimate.residuals)) if estimate.residuals else 0.0
    mad = float(median(abs(value - residual_center) for value in estimate.residuals)) if estimate.residuals else 0.0
    if mad > 1e-12:
        method, score = "modified_z_score", abs(0.67448975 * deviation / mad)
        warning, critical, units = policy.warning_z, policy.critical_z, "modified z-score"
        warning_distance = warning * mad / 0.67448975
        critical_distance = critical * mad / 0.67448975
    elif baseline != 0:
        method, score = "percentage_deviation", abs(deviation / baseline)
        warning, critical, units = policy.warning_ratio, policy.critical_ratio, "ratio"
        warning_distance, critical_distance = abs(baseline) * warning, abs(baseline) * critical
    else:
        method, score = "absolute_deviation", abs(deviation)
        warning, critical, units = policy.warning_absolute, policy.critical_absolute, "units"
        warning_distance, critical_distance = warning, critical
    warning_distance = max(warning_distance, policy.minimum_absolute_deviation)
    critical_distance = max(critical_distance, policy.minimum_absolute_deviation)
    anomalous = abs(deviation) >= warning_distance
    severity = Severity.CRITICAL if abs(deviation) >= critical_distance else Severity.WARNING if anomalous else Severity.INFO
    status = AnomalyStatus.ANOMALY if anomalous else AnomalyStatus.NORMAL
    lower_bound, upper_bound = baseline - warning_distance, baseline + warning_distance
    threshold.update({"lower_bound": lower_bound, "upper_bound": upper_bound})
    strategy_label = estimate.strategy.replace("_", " + " if estimate.strategy == "seasonal_trend" else " ")
    position = None if deviation_percent is None else (
        f"{abs(deviation_percent):.2f}% {'below' if deviation < 0 else 'above'} the expected range"
        if anomalous else f"within the expected range ({abs(deviation_percent):.2f}% "
                           f"{'below' if deviation < 0 else 'above'} expected)")
    explanation = (f"Current {series.metric_name} is {position}. "
                   f"Expected: {baseline:g}. Observed: {series.current_value:g}. "
                   f"Range: [{lower_bound:g}, {upper_bound:g}]. "
                   f"Baseline: {strategy_label}. History: {estimate.training_window_size} observations"
                   f"{f', {estimate.seasonal_reference_count} same-weekday' if estimate.seasonal_reference_count else ''}. "
                   f"Deviation {deviation:g}; {method} {score:.4g} {units}; warning threshold {warning:g}, "
                   f"critical threshold {critical:g}.") if deviation_percent is not None else (
                   f"Current {series.current_value:g}; expected {baseline:g}; baseline {strategy_label}; deviation {deviation:g}; "
                   f"{method} {score:.4g} {units}; warning threshold {warning:g}, critical threshold {critical:g}.")
    if estimate.strategy == "robust_history" and policy.baseline_strategies == ("robust_history",):
        explanation = (f"Current {series.current_value:g}; median baseline {baseline:g}; deviation {deviation:g} "
                   f"({deviation_percent:.2f}%); {method} {score:.4g} {units}; "
                   f"warning threshold {warning:g}, critical threshold {critical:g}.") if deviation_percent is not None else (
                   f"Current {series.current_value:g}; median baseline {baseline:g}; deviation {deviation:g}; "
                   f"{method} {score:.4g} {units}; warning threshold {warning:g}, critical threshold {critical:g}.")
    confidence = _confidence(estimate, policy, fallback_used)
    details = {"median_absolute_deviation": mad, "score": score,
               "requested_baseline_strategy": policy.baseline_strategies[0],
               "attempted_baseline_strategies": list(attempted),
               "baseline_strategy": estimate.strategy, "expected_value": baseline,
               "lower_bound": lower_bound, "upper_bound": upper_bound,
               "trend_slope": estimate.trend_slope,
               "seasonal_reference_count": estimate.seasonal_reference_count,
               "training_window_size": estimate.training_window_size,
               "model_error": estimate.model_error, "residual": deviation,
               "fallback_used": fallback_used, "confidence": confidence.value,
               "minimum_sample_size": policy.minimum_sample_size,
               "current_sample_size": series.current_sample_size,
               "raw_history_count": raw_history_count}
    return AnomalyResult(**common, baseline_value=baseline, deviation_value=deviation,
                         deviation_percent=deviation_percent, method=method, status=status,
                         severity=severity, explanation=explanation,
                         details=details, baseline_strategy=estimate.strategy, expected_value=baseline,
                         lower_bound=lower_bound, upper_bound=upper_bound,
                         trend_slope=estimate.trend_slope,
                         seasonal_reference_count=estimate.seasonal_reference_count,
                         training_window_size=estimate.training_window_size,
                         model_error=estimate.model_error, residual=deviation,
                         fallback_used=fallback_used, confidence=confidence)
