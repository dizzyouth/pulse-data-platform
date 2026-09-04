"""Curate Bronze marketplace events into quality-controlled Silver Parquet."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from src.producers.models import EventType
from src.streaming.windows_spark import (
    configure_windows_spark_builder,
    configure_windows_spark_environment,
)

# Configure the Windows process before PySpark can launch its JVM.
configure_windows_spark_environment()

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.types import (
    ArrayType,
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
DEFAULT_BRONZE_SOURCE_PATH = "data/bronze/marketplace_events/valid"
DEFAULT_SILVER_VALID_PATH = "data/silver/marketplace_events/valid"
DEFAULT_SILVER_REJECTED_PATH = "data/silver/marketplace_events/rejected"
DEFAULT_SILVER_VALID_CHECKPOINT_PATH = (
    "data/checkpoints/silver/marketplace_events/valid"
)
DEFAULT_SILVER_REJECTED_CHECKPOINT_PATH = (
    "data/checkpoints/silver/marketplace_events/rejected"
)
DEFAULT_SILVER_EVENT_WATERMARK = "7 days"
DEFAULT_SILVER_SHUFFLE_PARTITIONS = "4"

SUPPORTED_EVENT_TYPES = tuple(event_type.value for event_type in EventType)

MARKETPLACE_FIELDS = (
    "event_id",
    "event_type",
    "event_timestamp",
    "customer_id",
    "session_id",
    "country",
    "product_id",
    "seller_id",
    "order_id",
    "payment_id",
    "quantity",
    "unit_price",
    "currency",
)
LINEAGE_FIELDS = (
    "kafka_key",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at_utc",
)

BRONZE_VALID_SCHEMA = StructType(
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
        StructField("raw_json", StringType(), True),
        StructField(
            "validation_errors", ArrayType(StringType(), containsNull=False), False
        ),
        StructField("ingested_at_utc", TimestampType(), False),
        StructField("ingestion_date", DateType(), True),
    ]
)


@dataclass(frozen=True, slots=True)
class SilverPaths:
    """Resolved source, sink, and checkpoint paths for the Silver job."""

    bronze_source: Path
    valid: Path
    rejected: Path
    valid_checkpoint: Path
    rejected_checkpoint: Path
    event_watermark: str


@dataclass(frozen=True, slots=True)
class ClassifiedSilverFrames:
    """Quality-valid, rejected, and normalized Silver candidates."""

    valid: DataFrame
    rejected: DataFrame
    all_records: DataFrame


def _resolve_project_path(value: str, project_root: Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (project_root / path).resolve()


def load_silver_paths(
    environ: Mapping[str, str] | None = None,
    *,
    project_root: Path = PROJECT_ROOT,
) -> SilverPaths:
    """Load Silver configuration with project-relative path defaults."""

    environment = os.environ if environ is None else environ
    paths = SilverPaths(
        bronze_source=_resolve_project_path(
            environment.get(
                "BRONZE_MARKETPLACE_VALID_PATH", DEFAULT_BRONZE_SOURCE_PATH
            ),
            project_root,
        ),
        valid=_resolve_project_path(
            environment.get("SILVER_MARKETPLACE_VALID_PATH", DEFAULT_SILVER_VALID_PATH),
            project_root,
        ),
        rejected=_resolve_project_path(
            environment.get(
                "SILVER_MARKETPLACE_REJECTED_PATH", DEFAULT_SILVER_REJECTED_PATH
            ),
            project_root,
        ),
        valid_checkpoint=_resolve_project_path(
            environment.get(
                "SILVER_MARKETPLACE_VALID_CHECKPOINT_PATH",
                DEFAULT_SILVER_VALID_CHECKPOINT_PATH,
            ),
            project_root,
        ),
        rejected_checkpoint=_resolve_project_path(
            environment.get(
                "SILVER_MARKETPLACE_REJECTED_CHECKPOINT_PATH",
                DEFAULT_SILVER_REJECTED_CHECKPOINT_PATH,
            ),
            project_root,
        ),
        event_watermark=environment.get(
            "SILVER_EVENT_WATERMARK", DEFAULT_SILVER_EVENT_WATERMARK
        ),
    )
    if paths.valid == paths.rejected:
        raise ValueError("Silver valid and rejected output paths must be different")
    if paths.valid_checkpoint == paths.rejected_checkpoint:
        raise ValueError("Silver valid and rejected checkpoints must be different")
    if not paths.event_watermark.strip():
        raise ValueError("Silver event watermark cannot be empty")
    return paths


def build_silver_spark_session(
    *,
    app_name: str = "pulse-marketplace-silver",
    master: str = "local[*]",
) -> SparkSession:
    """Create a local Spark session for the Parquet-to-Parquet Silver job."""

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv(
                "SILVER_SHUFFLE_PARTITIONS", DEFAULT_SILVER_SHUFFLE_PARTITIONS
            ),
        )
    )
    return configure_windows_spark_builder(builder).getOrCreate()


def read_bronze_valid_stream(spark: SparkSession, source_path: Path) -> DataFrame:
    """Read the partitioned Bronze valid Parquet dataset as a stream."""

    return (
        spark.readStream.schema(BRONZE_VALID_SCHEMA)
        .option("basePath", str(source_path))
        .parquet(str(source_path))
    )


def _normalized_optional_string(name: str):
    trimmed = F.trim(F.col(name))
    return F.when(trimmed == "", F.lit(None).cast("string")).otherwise(trimmed)


def classify_silver_events(
    bronze_valid: DataFrame,
    *,
    event_watermark: str = DEFAULT_SILVER_EVENT_WATERMARK,
    deduplicate: bool = True,
) -> ClassifiedSilverFrames:
    """Normalize Bronze rows, apply Silver quality rules, and route failures."""

    normalized = bronze_valid.select(
        F.trim("event_id").alias("event_id"),
        F.lower(F.trim("event_type")).alias("event_type"),
        F.col("event_timestamp").cast("timestamp").alias("event_timestamp"),
        F.trim("customer_id").alias("customer_id"),
        F.trim("session_id").alias("session_id"),
        F.upper(_normalized_optional_string("country")).alias("country"),
        _normalized_optional_string("product_id").alias("product_id"),
        _normalized_optional_string("seller_id").alias("seller_id"),
        _normalized_optional_string("order_id").alias("order_id"),
        _normalized_optional_string("payment_id").alias("payment_id"),
        F.col("quantity").cast("integer").alias("quantity"),
        F.col("unit_price").cast("double").alias("unit_price"),
        F.upper(_normalized_optional_string("currency")).alias("currency"),
        *LINEAGE_FIELDS,
        "raw_json",
    ).withColumn("event_date", F.to_date("event_timestamp"))

    quality_errors = F.array_compact(
        F.array(
            F.when(
                F.col("event_id").isNull() | (F.col("event_id") == ""),
                F.lit("missing_event_id"),
            ),
            F.when(
                F.col("customer_id").isNull() | (F.col("customer_id") == ""),
                F.lit("missing_customer_id"),
            ),
            F.when(
                F.col("session_id").isNull() | (F.col("session_id") == ""),
                F.lit("missing_session_id"),
            ),
            F.when(
                F.col("event_timestamp").isNull(), F.lit("invalid_event_timestamp")
            ),
            F.when(
                ~F.col("event_type").isin(*SUPPORTED_EVENT_TYPES),
                F.lit("unsupported_event_type"),
            ),
            F.when(
                F.col("quantity").isNotNull() & (F.col("quantity") <= 0),
                F.lit("invalid_quantity"),
            ),
            F.when(
                F.col("unit_price").isNotNull() & (F.col("unit_price") < 0),
                F.lit("invalid_unit_price"),
            ),
            F.when(
                F.col("country").isNotNull()
                & ~F.col("country").rlike("^[A-Z]{2}$"),
                F.lit("invalid_country_code"),
            ),
            F.when(
                F.col("currency").isNotNull()
                & ~F.col("currency").rlike("^[A-Z]{3}$"),
                F.lit("invalid_currency_code"),
            ),
        )
    )
    classified = normalized.withColumn("silver_validation_errors", quality_errors)
    valid = classified.filter(F.size("silver_validation_errors") == 0).select(
        *MARKETPLACE_FIELDS, *LINEAGE_FIELDS, "event_date"
    )
    if deduplicate:
        valid = valid.withWatermark(
            "event_timestamp", event_watermark
        ).dropDuplicatesWithinWatermark(["event_id"])
    rejected = classified.filter(F.size("silver_validation_errors") > 0)
    return ClassifiedSilverFrames(valid=valid, rejected=rejected, all_records=classified)


def build_silver_writer(
    frame: DataFrame,
    *,
    output_path: Path,
    checkpoint_path: Path,
) -> DataStreamWriter:
    """Build an append-only, event-date-partitioned Silver Parquet writer."""

    return (
        frame.writeStream.format("parquet")
        .outputMode("append")
        .partitionBy("event_date")
        .option("path", str(output_path))
        .option("checkpointLocation", str(checkpoint_path))
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Curate Bronze marketplace events into Silver Parquet"
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="run continuously instead of processing currently available data",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    paths = load_silver_paths()
    master = os.getenv("SPARK_MASTER", "local[*]")

    spark = build_silver_spark_session(master=master)
    spark.sparkContext.setLogLevel("WARN")
    try:
        bronze = read_bronze_valid_stream(spark, paths.bronze_source)
        frames = classify_silver_events(
            bronze, event_watermark=paths.event_watermark
        )
        valid_writer = build_silver_writer(
            frames.valid,
            output_path=paths.valid,
            checkpoint_path=paths.valid_checkpoint,
        )
        rejected_writer = build_silver_writer(
            frames.rejected,
            output_path=paths.rejected,
            checkpoint_path=paths.rejected_checkpoint,
        )
        if args.continuous:
            valid_query = valid_writer.trigger(processingTime="5 seconds").start()
            rejected_query = rejected_writer.trigger(processingTime="5 seconds").start()
        else:
            valid_query = valid_writer.trigger(availableNow=True).start()
            rejected_query = rejected_writer.trigger(availableNow=True).start()

        valid_query.awaitTermination()
        rejected_query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping Silver streaming queries...")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
