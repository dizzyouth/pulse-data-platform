"""Deterministic, dependency-free baseline strategies for anomaly evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from src.quality.anomaly import AnomalyPolicy, MetricSeries


@dataclass(frozen=True, slots=True, kw_only=True)
class BaselineEstimate:
    strategy: str
    expected_value: float
    residuals: tuple[float, ...]
    training_window_size: int
    trend_slope: float | None = None
    seasonal_reference_count: int = 0

    @property
    def model_error(self) -> float:
        """Median absolute training residual; an operational error measure."""
        if not self.residuals:
            return 0.0
        center = median(self.residuals)
        return float(median(abs(value - center) for value in self.residuals))


class BaselineStrategy(Protocol):
    name: str

    def estimate(self, series: "MetricSeries", policy: "AnomalyPolicy") -> BaselineEstimate | None:
        """Return an estimate, or None when this strategy lacks usable history."""


def _window(series: "MetricSeries", size: int):
    values = series.history[-size:]
    if series.history_observed_at_utc:
        timestamps = series.history_observed_at_utc[-size:]
        origin = timestamps[0]
        x_values = tuple((stamp - origin).total_seconds() / 86400 for stamp in timestamps)
        current_x = (series.observed_at_utc - origin).total_seconds() / 86400
    else:
        x_values = tuple(float(index) for index in range(len(values)))
        current_x = float(len(values))
    return values, x_values, current_x


def _robust_line(values: tuple[float, ...], x_values: tuple[float, ...]):
    slopes = tuple(
        (values[right] - values[left]) / (x_values[right] - x_values[left])
        for left in range(len(values))
        for right in range(left + 1, len(values))
        if x_values[right] != x_values[left]
    )
    if not slopes:
        return None
    slope = float(median(slopes))
    intercept = float(median(value - slope * x for value, x in zip(values, x_values)))
    return slope, intercept


class RobustHistoryBaseline:
    name = "robust_history"

    def estimate(self, series, policy):
        if len(series.history) < policy.minimum_history:
            return None
        values = series.history[-policy.maximum_training_window:]
        expected = float(median(values))
        return BaselineEstimate(strategy=self.name, expected_value=expected,
                                residuals=tuple(value - expected for value in values),
                                training_window_size=len(values))


class DayOfWeekBaseline:
    name = "day_of_week"

    def estimate(self, series, policy):
        if not series.history_observed_at_utc:
            return None
        references = tuple(
            value for value, stamp in zip(series.history, series.history_observed_at_utc)
            if stamp.weekday() == series.observed_at_utc.weekday()
        )[-policy.maximum_training_window:]
        if len(references) < policy.minimum_same_weekday_history:
            return None
        expected = float(median(references))
        return BaselineEstimate(strategy=self.name, expected_value=expected,
                                residuals=tuple(value - expected for value in references),
                                training_window_size=len(references),
                                seasonal_reference_count=len(references))


class TrendBaseline:
    name = "trend"

    def estimate(self, series, policy):
        if len(series.history) < policy.minimum_trend_history:
            return None
        size = min(len(series.history), policy.maximum_training_window)
        values, x_values, current_x = _window(series, size)
        line = _robust_line(values, x_values)
        if line is None:
            return None
        slope, intercept = line
        expected = intercept + slope * current_x
        residuals = tuple(value - (intercept + slope * x) for value, x in zip(values, x_values))
        return BaselineEstimate(strategy=self.name, expected_value=float(expected), residuals=residuals,
                                training_window_size=size, trend_slope=slope)


class SeasonalTrendBaseline:
    name = "seasonal_trend"

    def estimate(self, series, policy):
        if (not series.history_observed_at_utc or
                len(series.history) < policy.minimum_seasonal_trend_history):
            return None
        size = min(len(series.history), policy.maximum_training_window)
        values, x_values, current_x = _window(series, size)
        stamps = series.history_observed_at_utc[-size:]
        line = _robust_line(values, x_values)
        if line is None:
            return None
        slope, _ = line
        indices = tuple(index for index, stamp in enumerate(stamps)
                        if stamp.weekday() == series.observed_at_utc.weekday())
        if len(indices) < policy.minimum_same_weekday_history:
            return None
        seasonal_level = float(median(values[index] - slope * x_values[index] for index in indices))
        expected = seasonal_level + slope * current_x
        residuals = tuple(values[index] - (seasonal_level + slope * x_values[index]) for index in indices)
        return BaselineEstimate(strategy=self.name, expected_value=float(expected), residuals=residuals,
                                training_window_size=size, trend_slope=slope,
                                seasonal_reference_count=len(indices))


STRATEGIES: dict[str, BaselineStrategy] = {
    strategy.name: strategy for strategy in (
        RobustHistoryBaseline(), DayOfWeekBaseline(), TrendBaseline(), SeasonalTrendBaseline()
    )
}

