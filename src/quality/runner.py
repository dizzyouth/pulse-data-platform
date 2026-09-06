"""One quality engine and CLI for local datasets and pipeline boundaries."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path
import os

from pyspark.sql import DataFrame, functions as F

from src.quality.checks import (
    aggregate_expression, column, evaluate_metric, result, schema_problem, validate_rule,
)
from src.quality.models import (
    CheckDetails, Freshness, QualityContext, QualityResult, Rule, Severity,
    Uniqueness, VolumeChange, report_json, should_block,
)


def run_quality_checks(
    dataset: DataFrame, rules: Sequence[Rule], context: QualityContext,
) -> list[QualityResult]:
    """Assess a stable batch snapshot without changing or automatically caching it.

    Missing/wrong columns produce check results. Invalid rule configuration,
    streaming inputs, and Spark execution failures raise exceptions.
    """
    if dataset.isStreaming:
        raise ValueError("Quality checks need a bounded batch DataFrame; use a snapshot or foreachBatch")
    if len(dataset.columns) != len(set(dataset.columns)):
        raise ValueError("Quality input has ambiguous duplicate column names")
    rules = tuple(rules)
    for rule in rules:
        validate_rule(rule)
    if len({rule.check_name for rule in rules}) != len(rules):
        raise ValueError("check_name must be unique within a quality run")
    if not rules:
        return []
    problems = {i: problem for i, rule in enumerate(rules) if (problem := schema_problem(dataset, rule))}
    expressions = [F.count(F.lit(1)).alias("_rows")]
    for i, rule in enumerate(rules):
        if i not in problems:
            expression = aggregate_expression(rule)
            if expression is not None:
                expressions.append(expression.alias(f"_metric_{i}"))
    metrics = dataset.agg(*expressions).first().asDict()
    rows = metrics["_rows"]
    duplicates: dict[tuple[str, ...], int] = {}
    results = []
    for i, rule in enumerate(rules):
        if i in problems:
            results.append(result(rule, context, metric="schema_valid", observed=0,
                                  expected=1, passed=False,
                                  details=CheckDetails(row_count=rows, message=problems[i])))
            continue
        metric = metrics.get(f"_metric_{i}")
        if isinstance(rule, Uniqueness):
            if rule.columns not in duplicates:
                # groupBy includes null key components; completeness is a separate rule.
                keys = [column(name).alias(f"_key_{n}") for n, name in enumerate(rule.columns)]
                grouped = dataset.select(*keys).groupBy(*(f"_key_{n}" for n in range(len(keys)))).count()
                duplicates[rule.columns] = grouped.agg(
                    F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("duplicates")
                ).first()["duplicates"]
            metric = duplicates[rule.columns]
        results.append(evaluate_metric(rule, context, rows, metric))
    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only quality assessment of Pulse Parquet or warehouse snapshots")
    parser.add_argument("dataset", choices=("silver", "gold", "warehouse", "silver_valid", "daily_sales", "customer_metrics", "product_metrics", "funnel_metrics"))
    parser.add_argument("--path", type=Path, help="override the configured local Parquet directory")
    parser.add_argument("--max-age-hours", type=float, help="optional Silver freshness warning threshold")
    parser.add_argument("--reference-count", type=int, help="explicit comparable previous snapshot count")
    parser.add_argument("--max-volume-change", type=float, help="relative deviation fraction (default 0.2 = 20%%); requires --reference-count")
    parser.add_argument("--block-on-critical", action="store_true", help="exit 1 on critical quality failures; default is report-only")
    parser.add_argument("--log-format", choices=("json", "jsonl"), default="json", help="JSON report (default) or flushed per-check log events and summary")
    return parser


def iter_target_results(spark, target: str, *, path: Path | None = None,
                        extra_rules: Sequence[Rule] = (), reference_count: int | None = None):
    """Select the same Phase 5.1 policies for every caller; emit each dataset's results."""
    from src.analytics.gold_build import load_gold_paths
    from src.streaming.silver_streaming import load_silver_paths
    from src.quality.datasets import GOLD_GRAINS, gold_rules, silver_rules
    from src.utils.parquet import read_parquet_data_files

    if target in ("gold", "warehouse") and (path is not None or extra_rules or reference_count is not None):
        raise ValueError("Path and optional thresholds require a single dataset")
    if target == "warehouse":
        from src.quality.warehouse import warehouse_frames
        with warehouse_frames(spark) as frames:
            for name, frame in frames.items():
                yield from run_quality_checks(frame, gold_rules(name, layer="analytics"),
                                              QualityContext(dataset_name=name, layer="analytics"))
        return
    names = tuple(GOLD_GRAINS) if target == "gold" else ("silver_valid" if target == "silver" else target,)
    for name in names:
        layer = "silver" if name == "silver_valid" else "gold"
        rules = (*silver_rules(), *extra_rules) if layer == "silver" else (*gold_rules(name), *extra_rules)
        source = path or (load_silver_paths().valid if layer == "silver" else getattr(load_gold_paths(), name))
        yield from run_quality_checks(read_parquet_data_files(spark, source.resolve()), rules,
                                      QualityContext(dataset_name=name, layer=layer, reference_count=reference_count))


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.max_volume_change is not None and args.reference_count is None:
        parser.error("--max-volume-change requires --reference-count")
    if args.dataset in ("gold", "warehouse") and any(value is not None for value in
            (args.path, args.max_age_hours, args.reference_count, args.max_volume_change)):
        parser.error("path and optional thresholds require a single dataset")
    from src.analytics.gold_build import build_gold_spark_session
    from src.quality.observability import emit_event, log_result, log_summary

    rules = []
    if args.max_age_hours is not None:
        if args.dataset not in ("silver", "silver_valid"):
            parser.error("--max-age-hours applies to Silver event_timestamp, not Gold aggregate dates")
        rules.append(Freshness(check_name="event_freshness", column="event_timestamp",
                               max_age_seconds=args.max_age_hours * 3600, severity=Severity.WARNING))
    if args.reference_count is not None:
        rules.append(VolumeChange(check_name="volume_change", max_deviation_ratio=0.2 if args.max_volume_change is None else args.max_volume_change,
                                  severity=Severity.WARNING))
    for rule in rules:
        validate_rule(rule)
    # Validate reference input before starting Spark, including programmatic CLI use.
    if args.reference_count is not None and args.reference_count < 0:
        parser.error("--reference-count must be nonnegative")
    spark = None
    results = []
    try:
        spark = build_gold_spark_session(app_name="pulse-data-quality", master=os.getenv("SPARK_MASTER", "local[2]"))
        spark.sparkContext.setLogLevel("ERROR")
        for result in iter_target_results(spark, args.dataset, path=args.path,
                                          extra_rules=rules, reference_count=args.reference_count):
            results.append(result)
            if args.log_format == "jsonl":
                log_result(result)
        if args.log_format == "jsonl":
            log_summary(results, target=args.dataset)
        else:
            print(report_json(results))
        return int(args.block_on_critical and should_block(results))
    except Exception as error:
        if args.log_format == "jsonl":
            emit_event("quality_execution_error", target=args.dataset,
                       error_type=type(error).__name__, message=str(error))
            log_summary(results, target=args.dataset, completed=False)
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
