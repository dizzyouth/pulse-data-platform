"""Build business-ready Gold aggregates from Silver marketplace events."""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from src.streaming.windows_spark import (
    configure_windows_spark_builder,
    configure_windows_spark_environment,
)
from src.utils.parquet import read_parquet_data_files

# Configure the Windows process before PySpark can launch its JVM.
configure_windows_spark_environment()

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SILVER_SOURCE_PATH = "data/silver/marketplace_events/valid"
DEFAULT_GOLD_DAILY_SALES_PATH = "data/gold/daily_sales"
DEFAULT_GOLD_CUSTOMER_METRICS_PATH = "data/gold/customer_metrics"
DEFAULT_GOLD_PRODUCT_METRICS_PATH = "data/gold/product_metrics"
DEFAULT_GOLD_FUNNEL_METRICS_PATH = "data/gold/funnel_metrics"
DEFAULT_GOLD_SHUFFLE_PARTITIONS = "4"
GOLD_OUTPUT_CLEANUP_ATTEMPTS = 5
GOLD_OUTPUT_CLEANUP_DELAY_SECONDS = 0.1

SILVER_VALID_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("customer_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("payment_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("kafka_key", StringType(), True),
        StructField("kafka_topic", StringType(), True),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset", LongType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("ingested_at_utc", TimestampType(), True),
        StructField("event_date", DateType(), True),
    ]
)


@dataclass(frozen=True, slots=True)
class GoldPaths:
    """Resolved Silver input and Gold output locations."""

    silver_source: Path
    daily_sales: Path
    customer_metrics: Path
    product_metrics: Path
    funnel_metrics: Path


@dataclass(frozen=True, slots=True)
class GoldTables:
    """The four Gold analytics tables produced from Silver."""

    daily_sales: DataFrame
    customer_metrics: DataFrame
    product_metrics: DataFrame
    funnel_metrics: DataFrame


def _resolve_project_path(value: str, project_root: Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (project_root / path).resolve()


def load_gold_paths(
    environ: Mapping[str, str] | None = None,
    *,
    project_root: Path = PROJECT_ROOT,
) -> GoldPaths:
    """Load Gold locations with project-relative defaults."""

    environment = os.environ if environ is None else environ
    paths = GoldPaths(
        silver_source=_resolve_project_path(
            environment.get(
                "SILVER_MARKETPLACE_VALID_PATH", DEFAULT_SILVER_SOURCE_PATH
            ),
            project_root,
        ),
        daily_sales=_resolve_project_path(
            environment.get("GOLD_DAILY_SALES_PATH", DEFAULT_GOLD_DAILY_SALES_PATH),
            project_root,
        ),
        customer_metrics=_resolve_project_path(
            environment.get(
                "GOLD_CUSTOMER_METRICS_PATH", DEFAULT_GOLD_CUSTOMER_METRICS_PATH
            ),
            project_root,
        ),
        product_metrics=_resolve_project_path(
            environment.get(
                "GOLD_PRODUCT_METRICS_PATH", DEFAULT_GOLD_PRODUCT_METRICS_PATH
            ),
            project_root,
        ),
        funnel_metrics=_resolve_project_path(
            environment.get(
                "GOLD_FUNNEL_METRICS_PATH", DEFAULT_GOLD_FUNNEL_METRICS_PATH
            ),
            project_root,
        ),
    )
    outputs = {
        paths.daily_sales,
        paths.customer_metrics,
        paths.product_metrics,
        paths.funnel_metrics,
    }
    if len(outputs) != 4:
        raise ValueError("Gold output paths must be distinct")
    if paths.silver_source in outputs:
        raise ValueError("Silver source path cannot also be a Gold output path")
    project_root = project_root.resolve()
    for output in outputs:
        if output == Path(output.anchor) or output == project_root:
            raise ValueError(f"Unsafe Gold output path: {output}")
        if paths.silver_source.is_relative_to(output) or output.is_relative_to(
            paths.silver_source
        ):
            raise ValueError("Silver source and Gold output paths cannot overlap")
    for output in outputs:
        if any(
            output != other
            and (output.is_relative_to(other) or other.is_relative_to(output))
            for other in outputs
        ):
            raise ValueError("Gold output paths cannot overlap")
    return paths


def build_gold_spark_session(
    *,
    app_name: str = "pulse-marketplace-gold",
    master: str = "local[*]",
) -> SparkSession:
    """Create a project-local Spark session for Gold batch aggregation."""

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv("GOLD_SHUFFLE_PARTITIONS", DEFAULT_GOLD_SHUFFLE_PARTITIONS),
        )
    )
    return configure_windows_spark_builder(builder).getOrCreate()


def read_silver_valid(spark: SparkSession, source_path: Path) -> DataFrame:
    """Read only the curated Silver valid Parquet dataset."""

    if not source_path.exists():
        raise FileNotFoundError(f"Silver valid path does not exist: {source_path}")
    return read_parquet_data_files(
        spark,
        source_path,
        schema=SILVER_VALID_SCHEMA,
    )


def _event_count(event_type: str) -> Column:
    return F.sum(F.when(F.col("event_type") == event_type, 1).otherwise(0)).cast(
        "long"
    )


def _payment_units() -> Column:
    return F.when(
        F.col("event_type") == "payment_completed", F.coalesce("quantity", F.lit(0))
    ).otherwise(0)


def _payment_revenue() -> Column:
    return F.when(
        F.col("event_type") == "payment_completed",
        F.coalesce(F.col("quantity").cast("double"), F.lit(0.0))
        * F.coalesce(F.col("unit_price"), F.lit(0.0)),
    ).otherwise(0.0)


def build_daily_sales(silver_valid: DataFrame) -> DataFrame:
    """Aggregate successful payment events by date, country, and currency."""

    daily = (
        silver_valid.filter(F.col("event_type") == "payment_completed")
        .groupBy("event_date", "country", "currency")
        .agg(
            F.countDistinct("order_id").cast("long").alias("completed_orders"),
            F.coalesce(F.sum(_payment_units()), F.lit(0)).cast("long").alias(
                "units_sold"
            ),
            F.coalesce(F.sum(_payment_revenue()), F.lit(0.0)).cast("double").alias(
                "gross_revenue"
            ),
        )
    )
    return daily.withColumn(
        "avg_order_value",
        F.when(
            F.col("completed_orders") > 0,
            F.col("gross_revenue") / F.col("completed_orders"),
        ).cast("double"),
    )


def build_customer_metrics(silver_valid: DataFrame) -> DataFrame:
    """Build one lifetime-to-date metrics row per customer."""

    return silver_valid.groupBy("customer_id").agg(
        F.min("event_timestamp").alias("first_event_at"),
        F.max("event_timestamp").alias("last_event_at"),
        _event_count("product_viewed").alias("products_viewed"),
        _event_count("product_added_to_cart").alias("cart_adds"),
        _event_count("checkout_started").alias("checkouts_started"),
        _event_count("order_created").alias("orders_created"),
        _event_count("payment_completed").alias("payments_completed"),
        _event_count("order_delivered").alias("orders_delivered"),
        _event_count("order_refunded").alias("orders_refunded"),
        F.coalesce(F.sum(_payment_units()), F.lit(0)).cast("long").alias(
            "total_units_purchased"
        ),
        F.coalesce(F.sum(_payment_revenue()), F.lit(0.0)).cast("double").alias(
            "total_revenue"
        ),
        F.countDistinct(
            F.when(F.col("event_type") == "payment_completed", F.col("order_id"))
        )
        .cast("long")
        .alias("distinct_orders"),
    )


def build_product_metrics(silver_valid: DataFrame) -> DataFrame:
    """Build one metrics row per product with recorded product activity."""

    metrics = (
        silver_valid.filter(F.col("product_id").isNotNull())
        .groupBy("product_id")
        .agg(
            F.collect_set("seller_id").alias("_seller_ids"),
            _event_count("product_viewed").alias("views"),
            _event_count("product_added_to_cart").alias("cart_adds"),
            _event_count("order_created").alias("orders_created"),
            _event_count("payment_completed").alias("payments_completed"),
            F.coalesce(F.sum(_payment_units()), F.lit(0)).cast("long").alias(
                "units_sold"
            ),
            F.coalesce(F.sum(_payment_revenue()), F.lit(0.0))
            .cast("double")
            .alias("gross_revenue"),
            F.countDistinct("customer_id").cast("long").alias("distinct_customers"),
        )
        .withColumn(
            "seller_id",
            F.when(F.size("_seller_ids") == 1, F.col("_seller_ids")[0]).cast(
                "string"
            ),
        )
    )
    return metrics.select(
        "product_id",
        "seller_id",
        "views",
        "cart_adds",
        "orders_created",
        "payments_completed",
        "units_sold",
        "gross_revenue",
        "distinct_customers",
    )


def _safe_rate(numerator: str, denominator: str) -> Column:
    return F.when(
        F.col(denominator) > 0,
        F.col(numerator).cast("double") / F.col(denominator),
    ).cast("double")


def build_funnel_metrics(silver_valid: DataFrame) -> DataFrame:
    """Aggregate event-count funnel stages by UTC event date and country."""

    funnel = silver_valid.groupBy("event_date", "country").agg(
        _event_count("product_viewed").alias("product_views"),
        _event_count("product_added_to_cart").alias("cart_adds"),
        _event_count("checkout_started").alias("checkouts_started"),
        _event_count("order_created").alias("orders_created"),
        _event_count("payment_completed").alias("payments_completed"),
        _event_count("order_delivered").alias("orders_delivered"),
        _event_count("order_refunded").alias("refunds"),
    )
    return (
        funnel.withColumn(
            "view_to_cart_rate", _safe_rate("cart_adds", "product_views")
        )
        .withColumn(
            "cart_to_checkout_rate", _safe_rate("checkouts_started", "cart_adds")
        )
        .withColumn(
            "checkout_to_order_rate", _safe_rate("orders_created", "checkouts_started")
        )
        .withColumn(
            "order_to_payment_rate", _safe_rate("payments_completed", "orders_created")
        )
    )


def build_gold_tables(silver_valid: DataFrame) -> GoldTables:
    """Build all Gold tables from the same Silver valid snapshot."""

    return GoldTables(
        daily_sales=build_daily_sales(silver_valid),
        customer_metrics=build_customer_metrics(silver_valid),
        product_metrics=build_product_metrics(silver_valid),
        funnel_metrics=build_funnel_metrics(silver_valid),
    )


def validate_gold_tables(tables: GoldTables) -> None:
    """Fail before writing when core aggregate sanity constraints are violated."""

    checks = {
        "daily_sales": tables.daily_sales.filter(
            (F.col("completed_orders") < 0)
            | (F.col("units_sold") < 0)
            | (F.col("gross_revenue") < 0)
        ),
        "customer_metrics": tables.customer_metrics.filter(
            (F.col("total_units_purchased") < 0) | (F.col("total_revenue") < 0)
        ),
        "product_metrics": tables.product_metrics.filter(
            (F.col("units_sold") < 0) | (F.col("gross_revenue") < 0)
        ),
        "funnel_metrics": tables.funnel_metrics.filter(
            F.exists(
                F.array(
                    "view_to_cart_rate",
                    "cart_to_checkout_rate",
                    "checkout_to_order_rate",
                    "order_to_payment_rate",
                ),
                lambda rate: rate.isNotNull() & ((rate < 0) | (rate > 1)),
            )
        ),
    }
    failures = [name for name, invalid in checks.items() if invalid.take(1)]
    if failures:
        raise ValueError(
            "Gold aggregate sanity checks failed for: " + ", ".join(failures)
        )


def _prepare_gold_output(output_path: Path, allowed_outputs: set[Path]) -> None:
    """Remove exactly one validated Gold destination before Spark writes it.

    Deleting before the writer starts avoids Hadoop's unreliable Windows
    overwrite cleanup. A bounded retry handles short-lived file handles without
    attempting to rewrite Windows ACLs or hiding a permanent access failure.
    """

    output = output_path.resolve()
    if output not in allowed_outputs:
        raise ValueError(f"Refusing to remove unconfigured Gold path: {output}")
    if not output.exists():
        return
    if not output.is_dir():
        raise ValueError(f"Gold output path is not a directory: {output}")

    for attempt in range(1, GOLD_OUTPUT_CLEANUP_ATTEMPTS + 1):
        try:
            shutil.rmtree(output)
            return
        except OSError as error:
            if attempt == GOLD_OUTPUT_CLEANUP_ATTEMPTS:
                raise OSError(
                    "Unable to remove existing Gold output after "
                    f"{GOLD_OUTPUT_CLEANUP_ATTEMPTS} attempts: {output}"
                ) from error
            time.sleep(GOLD_OUTPUT_CLEANUP_DELAY_SECONDS * (2 ** (attempt - 1)))


def write_gold_tables(tables: GoldTables, paths: GoldPaths) -> None:
    """Write a full, reproducible Gold snapshot as Parquet."""

    allowed_outputs = {
        path.resolve()
        for path in (
            paths.daily_sales,
            paths.customer_metrics,
            paths.product_metrics,
            paths.funnel_metrics,
        )
    }

    _prepare_gold_output(paths.daily_sales, allowed_outputs)
    tables.daily_sales.write.mode("overwrite").partitionBy("event_date").parquet(
        str(paths.daily_sales)
    )
    _prepare_gold_output(paths.customer_metrics, allowed_outputs)
    tables.customer_metrics.write.mode("overwrite").parquet(
        str(paths.customer_metrics)
    )
    _prepare_gold_output(paths.product_metrics, allowed_outputs)
    tables.product_metrics.write.mode("overwrite").parquet(str(paths.product_metrics))
    _prepare_gold_output(paths.funnel_metrics, allowed_outputs)
    tables.funnel_metrics.write.mode("overwrite").partitionBy("event_date").parquet(
        str(paths.funnel_metrics)
    )


def build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        description="Build Gold marketplace analytics from Silver valid Parquet"
    )


def main(argv: Sequence[str] | None = None) -> int:
    build_parser().parse_args(argv)
    paths = load_gold_paths()
    master = os.getenv("SPARK_MASTER", "local[*]")
    spark = build_gold_spark_session(master=master)
    spark.sparkContext.setLogLevel("WARN")
    silver_valid: DataFrame | None = None
    try:
        silver_valid = read_silver_valid(spark, paths.silver_source).cache()
        silver_valid.count()
        tables = build_gold_tables(silver_valid)
        validate_gold_tables(tables)
        write_gold_tables(tables, paths)
    finally:
        if silver_valid is not None:
            silver_valid.unpersist(blocking=True)
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
