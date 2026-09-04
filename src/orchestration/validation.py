"""Spark-backed validations used by the Airflow analytics workflow."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from src.analytics.gold_build import (
    GoldTables,
    build_gold_spark_session,
    load_gold_paths,
    validate_gold_tables,
)
from src.streaming.silver_streaming import load_silver_paths
from src.utils.parquet import read_parquet_data_files

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _read_required_parquet(
    spark: SparkSession,
    path: Path,
    *,
    label: str,
    required_columns: set[str],
) -> DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{label} path does not exist: {path}")
    try:
        frame = read_parquet_data_files(spark, path)
    except Exception as error:
        raise ValueError(f"{label} is not readable Parquet: {path}") from error
    missing = required_columns.difference(frame.columns)
    if missing:
        raise ValueError(f"{label} is missing columns: {', '.join(sorted(missing))}")
    return frame


def validate_bronze_available(spark: SparkSession, path: Path) -> int:
    """Require readable, non-empty Bronze valid Parquet."""

    frame = _read_required_parquet(
        spark,
        path,
        label="Bronze valid",
        required_columns={"event_id", "event_type", "raw_json"},
    )
    row_count = frame.count()
    if row_count == 0:
        raise ValueError("Bronze valid dataset is empty")
    return row_count


def validate_silver_output(
    spark: SparkSession,
    valid_path: Path,
    rejected_path: Path,
) -> Mapping[str, int | None]:
    """Validate Silver valid rows and allow an independent rejected dataset."""

    valid = _read_required_parquet(
        spark,
        valid_path,
        label="Silver valid",
        required_columns={"event_id", "event_type", "event_timestamp", "event_date"},
    )
    valid_count = valid.count()
    if valid_count == 0:
        raise ValueError("Silver valid dataset is empty")
    duplicate = valid.groupBy("event_id").count().filter(F.col("count") > 1).take(1)
    if duplicate:
        raise ValueError(f"Silver valid contains duplicate event_id: {duplicate[0][0]}")

    rejected_count: int | None = None
    if rejected_path.exists():
        parquet_files = tuple(rejected_path.rglob("*.parquet"))
        if parquet_files:
            rejected_count = spark.read.parquet(str(rejected_path)).count()
        else:
            rejected_count = 0
    return {"valid": valid_count, "rejected": rejected_count}


def validate_gold_output(spark: SparkSession, paths) -> Mapping[str, int]:
    """Require all Gold tables to be readable and pass existing sanity checks."""

    required = {
        "daily_sales": (
            paths.daily_sales,
            {"completed_orders", "units_sold", "gross_revenue", "avg_order_value"},
        ),
        "customer_metrics": (
            paths.customer_metrics,
            {"customer_id", "total_units_purchased", "total_revenue"},
        ),
        "product_metrics": (
            paths.product_metrics,
            {"product_id", "units_sold", "gross_revenue"},
        ),
        "funnel_metrics": (
            paths.funnel_metrics,
            {
                "event_date",
                "view_to_cart_rate",
                "cart_to_checkout_rate",
                "checkout_to_order_rate",
                "order_to_payment_rate",
            },
        ),
    }
    frames = {
        name: _read_required_parquet(
            spark, path, label=name, required_columns=columns
        )
        for name, (path, columns) in required.items()
    }
    validate_gold_tables(GoldTables(**frames))
    return {name: frame.count() for name, frame in frames.items()}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate Pulse pipeline datasets")
    parser.add_argument("layer", choices=("bronze", "silver", "gold"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    spark = build_gold_spark_session(
        app_name=f"pulse-validate-{args.layer}",
        master=os.getenv("SPARK_MASTER", "local[*]"),
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        silver_paths = load_silver_paths()
        if args.layer == "bronze":
            result = {"bronze_valid": validate_bronze_available(spark, silver_paths.bronze_source)}
        elif args.layer == "silver":
            result = validate_silver_output(
                spark, silver_paths.valid, silver_paths.rejected
            )
        else:
            result = validate_gold_output(spark, load_gold_paths())
        print(json.dumps(result, sort_keys=True))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
