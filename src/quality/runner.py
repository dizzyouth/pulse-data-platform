"""Run batch DataFrame checks, or assess one local Pulse Parquet dataset."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

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
    parser = argparse.ArgumentParser(description="Read-only quality assessment of local Pulse Parquet")
    parser.add_argument("dataset", choices=("silver_valid", "daily_sales", "customer_metrics", "product_metrics", "funnel_metrics"))
    parser.add_argument("--path", type=Path, help="override the configured local Parquet directory")
    parser.add_argument("--max-age-hours", type=float, help="optional Silver freshness warning threshold")
    parser.add_argument("--reference-count", type=int, help="explicit comparable previous snapshot count")
    parser.add_argument("--max-volume-change", type=float, help="relative deviation fraction (default 0.2 = 20%%); requires --reference-count")
    parser.add_argument("--block-on-critical", action="store_true", help="exit 1 on critical quality failures; default is report-only")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.max_volume_change is not None and args.reference_count is None:
        parser.error("--max-volume-change requires --reference-count")
    from src.analytics.gold_build import build_gold_spark_session, load_gold_paths
    from src.streaming.silver_streaming import load_silver_paths
    from src.quality.datasets import gold_rules, silver_rules
    from src.utils.parquet import read_parquet_data_files

    layer = "silver" if args.dataset == "silver_valid" else "gold"
    context = QualityContext(dataset_name=args.dataset, layer=layer, reference_count=args.reference_count)
    rules = list(silver_rules() if layer == "silver" else gold_rules(args.dataset))
    if args.max_age_hours is not None:
        if layer != "silver":
            parser.error("--max-age-hours applies to Silver event_timestamp, not Gold aggregate dates")
        rules.append(Freshness(check_name="event_freshness", column="event_timestamp",
                               max_age_seconds=args.max_age_hours * 3600, severity=Severity.WARNING))
    if args.reference_count is not None:
        rules.append(VolumeChange(check_name="volume_change", max_deviation_ratio=0.2 if args.max_volume_change is None else args.max_volume_change,
                                  severity=Severity.WARNING))
    for rule in rules:
        validate_rule(rule)
    path = args.path or (load_silver_paths().valid if layer == "silver" else getattr(load_gold_paths(), args.dataset))
    spark = build_gold_spark_session(app_name="pulse-data-quality", master="local[2]")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        results = run_quality_checks(read_parquet_data_files(spark, path.resolve()), rules, context)
        print(report_json(results))
        return int(args.block_on_critical and should_block(results))
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
