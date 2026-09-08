"""Walk-forward anomaly backtesting with strict prevention of future leakage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from uuid import NAMESPACE_URL, uuid5

from src.quality.anomaly import AnomalyPolicy, AnomalyResult, AnomalyStatus, MetricSeries, evaluate


@dataclass(frozen=True, slots=True, kw_only=True)
class BacktestReport:
    results: tuple[AnomalyResult, ...]
    evaluation_count: int
    anomaly_count: int
    insufficient_history_count: int
    alert_rate: float
    false_positive_proxy: int | None


def backtest_series(series: MetricSeries, policy: AnomalyPolicy,
                    truth_labels: tuple[bool, ...] = ()) -> BacktestReport:
    """Evaluate each point using only observations strictly before that point."""
    values = (*series.history, series.current_value)
    if series.history_observed_at_utc:
        timestamps = (*series.history_observed_at_utc, series.observed_at_utc)
    else:
        timestamps = tuple(series.observed_at_utc - timedelta(days=len(values) - index - 1)
                           for index in range(len(values)))
    samples = (*series.history_sample_sizes, series.current_sample_size)
    if not series.history_sample_sizes:
        samples = (None,) * (len(values) - 1) + (series.current_sample_size,)
    if truth_labels and len(truth_labels) != len(values):
        raise ValueError("truth_labels must align with all history and current observations")
    results = []
    for index, (value, observed_at) in enumerate(zip(values, timestamps)):
        prefix = MetricSeries(metric_name=series.metric_name, dataset_name=series.dataset_name,
                              layer=series.layer, dimensions=series.dimensions,
                              current_value=value, history=tuple(values[:index]),
                              observed_at_utc=observed_at,
                              history_observed_at_utc=tuple(timestamps[:index]),
                              current_sample_size=samples[index],
                              history_sample_sizes=tuple(samples[:index]))
        identity = uuid5(NAMESPACE_URL, "|".join((series.dataset_name, series.layer,
                         series.metric_name, observed_at.isoformat(), str(sorted(series.dimensions.items())))))
        results.append(evaluate(prefix, policy, identity))
    evaluated = tuple(item for item in results if item.status != AnomalyStatus.INSUFFICIENT_HISTORY)
    anomalies = sum(item.status == AnomalyStatus.ANOMALY for item in evaluated)
    false_positive = None
    if truth_labels:
        false_positive = sum(result.status == AnomalyStatus.ANOMALY and not truth
                             for result, truth in zip(results, truth_labels))
    return BacktestReport(results=tuple(results), evaluation_count=len(evaluated), anomaly_count=anomalies,
                          insufficient_history_count=len(results) - len(evaluated),
                          alert_rate=anomalies / len(evaluated) if evaluated else 0.0,
                          false_positive_proxy=false_positive)