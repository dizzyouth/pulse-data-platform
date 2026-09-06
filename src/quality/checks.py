"""Compile rules to Spark aggregations and interpret bounded scalar metrics."""

from __future__ import annotations

import math
import re
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import NumericType, StringType, TimestampType

from src.quality.models import (
    AllowedValues, CheckDetails, Freshness, NullRatio, NumericBounds, Pattern,
    QualityContext, QualityResult, RowCount, Rule, Severity, Status, Uniqueness,
    VolumeChange,
)


def column(name: str) -> Column:
    """Treat configured names as literal top-level columns, including dots."""
    return F.col("`" + name.replace("`", "``") + "`")


def _finite_nonnegative(value: float, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError(f"{label} must be finite and nonnegative")


def validate_rule(rule: Rule) -> None:
    if not isinstance(rule.check_name, str) or not rule.check_name.strip():
        raise ValueError("check_name is required")
    if not isinstance(rule.severity, Severity):
        raise ValueError("severity must be a Severity value")
    if isinstance(rule, RowCount):
        if type(rule.min_rows) is not int or rule.min_rows < 0:
            raise ValueError("min_rows must be a nonnegative integer")
    elif isinstance(rule, (NullRatio, Uniqueness)):
        ratio = rule.max_ratio if isinstance(rule, NullRatio) else rule.max_duplicate_ratio
        _finite_nonnegative(ratio, "ratio")
        if ratio > 1:
            raise ValueError("ratio must be <= 1")
        if isinstance(rule, Uniqueness) and (
            not isinstance(rule.columns, tuple) or not rule.columns or len(set(rule.columns)) != len(rule.columns)
        ):
            raise ValueError("uniqueness needs distinct key columns in a nonempty tuple")
    elif isinstance(rule, AllowedValues):
        if not isinstance(rule.values, tuple) or not rule.values or any(
            not isinstance(v, (str, int, float)) or (isinstance(v, float) and not math.isfinite(v))
            for v in rule.values
        ):
            raise ValueError("values must be a nonempty tuple of finite scalar values")
    elif isinstance(rule, NumericBounds):
        bounds = [v for v in (rule.minimum, rule.maximum) if v is not None]
        if not bounds or any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) for v in bounds):
            raise ValueError("numeric bounds need at least one finite limit")
        if rule.minimum is not None and rule.maximum is not None and (
            rule.minimum > rule.maximum or (rule.minimum == rule.maximum and not rule.minimum_inclusive)
        ):
            raise ValueError("numeric bounds define an empty interval")
    elif isinstance(rule, Pattern):
        if not rule.pattern:
            raise ValueError("pattern must be nonempty")
        re.compile(rule.pattern)
    elif isinstance(rule, Freshness):
        _finite_nonnegative(rule.max_age_seconds, "max_age_seconds")
        _finite_nonnegative(rule.max_future_seconds, "max_future_seconds")
    elif isinstance(rule, VolumeChange):
        _finite_nonnegative(rule.max_deviation_ratio, "max_deviation_ratio")
    else:
        raise TypeError(f"Unsupported quality rule: {type(rule).__name__}")
    for name in required_columns(rule):
        if not isinstance(name, str) or not name.strip():
            raise ValueError("column names must be nonempty strings")


def required_columns(rule: Rule) -> tuple[str, ...]:
    if isinstance(rule, Uniqueness):
        return rule.columns
    return (rule.column,) if hasattr(rule, "column") else ()


def schema_problem(frame: DataFrame, rule: Rule) -> str | None:
    missing = set(required_columns(rule)).difference(frame.columns)
    if missing:
        return f"Missing columns: {', '.join(sorted(missing))}"
    expected_type = (
        NumericType if isinstance(rule, NumericBounds) else
        TimestampType if isinstance(rule, Freshness) else
        StringType if isinstance(rule, Pattern) else None
    )
    if expected_type and not isinstance(frame.schema[rule.column].dataType, expected_type):
        return f"{rule.column} requires {expected_type.__name__}; no implicit quality cast"
    return None


def aggregate_expression(rule: Rule) -> Column | None:
    if isinstance(rule, (RowCount, VolumeChange, Uniqueness)):
        return None
    value = column(rule.column)
    if isinstance(rule, Freshness):
        # Epoch seconds preserve UTC independently of the Python/Spark session timezone.
        return F.max(value.cast("double"))
    if isinstance(rule, NullRatio):
        invalid = value.isNull()
    elif isinstance(rule, AllowedValues):
        invalid = ~value.isin(*rule.values)
    elif isinstance(rule, Pattern):
        invalid = ~value.rlike(rule.pattern)
    elif isinstance(rule, NumericBounds):
        invalid = F.isnan(value.cast("double")) | value.isin(float("inf"), float("-inf"))
        if rule.minimum is not None:
            invalid = invalid | (value < rule.minimum if rule.minimum_inclusive else value <= rule.minimum)
        if rule.maximum is not None:
            invalid = invalid | (value > rule.maximum)
    else:
        raise TypeError(type(rule).__name__)
    if hasattr(rule, "allow_null"):
        invalid = F.when(value.isNull(), F.lit(not rule.allow_null)).otherwise(invalid)
    return F.count(F.when(invalid, 1))


def result(
    rule: Rule, context: QualityContext, *, metric: str,
    observed: int | float | None, expected: int | float | str,
    passed: bool, details: CheckDetails, unavailable: bool = False,
) -> QualityResult:
    status = Status.WARN if unavailable else Status.PASS if passed else (
        Status.FAIL if rule.severity == Severity.CRITICAL else Status.WARN
    )
    return QualityResult(
        check_name=rule.check_name, dataset_name=context.dataset_name, layer=context.layer,
        severity=rule.severity, status=status, metric_name=metric,
        observed_value=observed, expected_value=expected,
        checked_at_utc=context.checked_at_utc, details=details,
    )


def evaluate_metric(rule: Rule, context: QualityContext, rows: int, metric: int | float | None) -> QualityResult:
    details = CheckDetails(row_count=rows)
    if isinstance(rule, RowCount):
        return result(rule, context, metric="row_count", observed=rows, expected=rule.min_rows,
                      passed=rows >= rule.min_rows, details=details)
    if isinstance(rule, VolumeChange):
        reference = context.reference_count
        deviation = None if reference in (None, 0) else abs(rows - reference) / reference
        if reference == 0 and rows == 0:
            deviation = 0.0
        details = CheckDetails(row_count=rows, reference_count=reference,
                               message="No reference supplied" if reference is None else
                               "Growth from zero has undefined relative deviation" if reference == 0 and rows else "")
        return result(rule, context, metric="volume_deviation_ratio", observed=deviation,
                      expected=rule.max_deviation_ratio, passed=deviation is not None and deviation <= rule.max_deviation_ratio,
                      unavailable=reference is None, details=details)
    if isinstance(rule, Freshness):
        latest = None if metric is None else datetime.fromtimestamp(metric, timezone.utc)
        age = None if metric is None else context.checked_at_utc.timestamp() - metric
        return result(rule, context, metric="latest_age_seconds", observed=age,
                      expected=f"[-{rule.max_future_seconds}, {rule.max_age_seconds}] seconds",
                      passed=age is not None and -rule.max_future_seconds <= age <= rule.max_age_seconds,
                      details=CheckDetails(row_count=rows, latest_timestamp_utc=latest,
                                           message="No non-null timestamp" if latest is None else ""))
    empty_message = "Empty sample; no evidence for this check" if rows == 0 else ""
    if isinstance(rule, NullRatio):
        ratio = metric / rows if rows else None
        return result(rule, context, metric="null_ratio", observed=ratio, expected=rule.max_ratio,
                      passed=ratio is not None and ratio <= rule.max_ratio, unavailable=rows == 0,
                      details=CheckDetails(row_count=rows, violating_rows=int(metric), message=empty_message))
    if isinstance(rule, Uniqueness):
        ratio = metric / rows if rows else None
        return result(rule, context, metric="duplicate_ratio", observed=ratio, expected=rule.max_duplicate_ratio,
                      passed=ratio is not None and ratio <= rule.max_duplicate_ratio, unavailable=rows == 0,
                      details=CheckDetails(row_count=rows, duplicate_count=int(metric), duplicate_rate=ratio, message=empty_message))
    expectation = (
        f"allowed={rule.values}; allow_null={rule.allow_null}" if isinstance(rule, AllowedValues) else
        f"pattern={rule.pattern}; allow_null={rule.allow_null}" if isinstance(rule, Pattern) else
        f"minimum={rule.minimum}; inclusive={rule.minimum_inclusive}; maximum={rule.maximum}; allow_null={rule.allow_null}"
    )
    return result(rule, context, metric="invalid_count", observed=metric, expected=expectation,
                  passed=metric == 0, unavailable=rows == 0,
                  details=CheckDetails(row_count=rows, violating_rows=int(metric), message=empty_message))
