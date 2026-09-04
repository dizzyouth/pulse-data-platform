"""Spark Structured Streaming foundation for Pulse marketplace events."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from src.streaming.windows_spark import (
    configure_windows_spark_builder,
    configure_windows_spark_environment,
)

# Configure Windows before importing PySpark, which may launch child processes.
# This mutates only this process's environment; it does not persist globally.
configure_windows_spark_environment()

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SPARK_VERSION = "4.2.0"
KAFKA_CONNECTOR_PACKAGE = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
)

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_MARKETPLACE_TOPIC = "marketplace.events"
DEFAULT_STARTING_OFFSETS = "earliest"
DEFAULT_BRONZE_VALID_PATH = "data/bronze/marketplace_events/valid"
DEFAULT_BRONZE_INVALID_PATH = "data/bronze/marketplace_events/invalid"
DEFAULT_BRONZE_VALID_CHECKPOINT_PATH = (
    "data/checkpoints/bronze/marketplace_events/valid"
)
DEFAULT_BRONZE_INVALID_CHECKPOINT_PATH = (
    "data/checkpoints/bronze/marketplace_events/invalid"
)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

MARKETPLACE_EVENT_SCHEMA = StructType(
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
    ]
)

_PARSING_SCHEMA = StructType(
    [
        *MARKETPLACE_EVENT_SCHEMA.fields,
        StructField("_corrupt_record", StringType(), True),
    ]
)


@dataclass(frozen=True, slots=True)
class ClassifiedMarketplaceFrames:
    """Logical valid and invalid event streams derived from Kafka records."""

    valid: DataFrame
    invalid: DataFrame
    all_records: DataFrame


@dataclass(frozen=True, slots=True)
class BronzePaths:
    """Resolved output and checkpoint locations for both Bronze streams."""

    valid: Path
    invalid: Path
    valid_checkpoint: Path
    invalid_checkpoint: Path


def _resolve_project_path(value: str, project_root: Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (project_root / path).resolve()


def load_bronze_paths(
    environ: Mapping[str, str] | None = None,
    *,
    project_root: Path = PROJECT_ROOT,
) -> BronzePaths:
    """Load Bronze locations, resolving relative values from the project root."""

    environment = os.environ if environ is None else environ
    paths = BronzePaths(
        valid=_resolve_project_path(
            environment.get("BRONZE_MARKETPLACE_VALID_PATH", DEFAULT_BRONZE_VALID_PATH),
            project_root,
        ),
        invalid=_resolve_project_path(
            environment.get(
                "BRONZE_MARKETPLACE_INVALID_PATH", DEFAULT_BRONZE_INVALID_PATH
            ),
            project_root,
        ),
        valid_checkpoint=_resolve_project_path(
            environment.get(
                "BRONZE_MARKETPLACE_VALID_CHECKPOINT_PATH",
                DEFAULT_BRONZE_VALID_CHECKPOINT_PATH,
            ),
            project_root,
        ),
        invalid_checkpoint=_resolve_project_path(
            environment.get(
                "BRONZE_MARKETPLACE_INVALID_CHECKPOINT_PATH",
                DEFAULT_BRONZE_INVALID_CHECKPOINT_PATH,
            ),
            project_root,
        ),
    )
    if paths.valid == paths.invalid:
        raise ValueError("Bronze valid and invalid output paths must be different")
    if paths.valid_checkpoint == paths.invalid_checkpoint:
        raise ValueError("Bronze valid and invalid checkpoint paths must be different")
    return paths


def build_spark_session(
    *,
    app_name: str = "pulse-marketplace-streaming",
    master: str = "local[*]",
) -> SparkSession:
    """Create local Spark with the version-matched Kafka connector package."""

    # Spark's Windows launcher otherwise looks specifically for ``python3``.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
    )
    builder = configure_windows_spark_builder(builder)
    connector_classpath = os.getenv("SPARK_KAFKA_CONNECTOR_CLASSPATH")
    if connector_classpath:
        # Windows fallback when Hadoop winutils is unavailable. The normal
        # cross-platform path below resolves the same coordinate through Ivy.
        builder = builder.config(
            "spark.driver.extraClassPath", connector_classpath
        ).config("spark.executor.extraClassPath", connector_classpath)
    else:
        builder = builder.config("spark.jars.packages", KAFKA_CONNECTOR_PACKAGE)
    return builder.getOrCreate()


def read_marketplace_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    *,
    starting_offsets: str = DEFAULT_STARTING_OFFSETS,
) -> DataFrame:
    """Create the unbounded Kafka source DataFrame."""

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )


def classify_marketplace_events(
    kafka_records: DataFrame,
) -> ClassifiedMarketplaceFrames:
    """Parse Kafka JSON and split records by the phase-3.1 validation rules."""

    decoded = kafka_records.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).withColumn(
        "parsed",
        F.from_json(
            F.col("raw_json"),
            _PARSING_SCHEMA,
            {
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record",
            },
        ),
    )

    missing_or_mismatched = F.array_compact(
        F.array(
            F.when(F.col("parsed.event_id").isNull(), F.lit("missing_event_id")),
            F.when(F.col("parsed.event_type").isNull(), F.lit("missing_event_type")),
            F.when(
                F.col("parsed.event_timestamp").isNull(),
                F.lit("missing_event_timestamp"),
            ),
            F.when(
                F.col("parsed.customer_id").isNull(), F.lit("missing_customer_id")
            ),
            F.when(
                F.col("kafka_key").isNull()
                | (
                    F.col("parsed.customer_id").isNotNull()
                    & (F.col("kafka_key") != F.col("parsed.customer_id"))
                ),
                F.lit("kafka_key_customer_id_mismatch"),
            ),
        )
    )
    validation_errors = F.when(
        F.col("parsed._corrupt_record").isNotNull(),
        F.array(F.lit("malformed_json")),
    ).otherwise(missing_or_mismatched)

    event_columns = [
        F.col(f"parsed.{field.name}").alias(field.name)
        for field in MARKETPLACE_EVENT_SCHEMA.fields
    ]
    classified = decoded.select(
        *event_columns,
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "raw_json",
        validation_errors.alias("validation_errors"),
    ).withColumn("ingested_at_utc", F.current_timestamp())
    classified = classified.withColumn(
        "ingestion_date", F.to_date(F.col("ingested_at_utc"))
    )
    valid = classified.filter(F.size("validation_errors") == 0)
    invalid = classified.filter(F.size("validation_errors") > 0)
    return ClassifiedMarketplaceFrames(valid=valid, invalid=invalid, all_records=classified)


def build_bronze_writer(
    frame: DataFrame,
    *,
    output_path: Path,
    checkpoint_path: Path,
):
    """Build an append-only, date-partitioned Parquet streaming writer."""

    return (
        frame.writeStream.format("parquet")
        .outputMode("append")
        .partitionBy("ingestion_date")
        .option("path", str(output_path))
        .option("checkpointLocation", str(checkpoint_path))
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read and classify Pulse marketplace events with Spark"
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="run continuously instead of processing currently available data",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS
    )
    topic = os.getenv("KAFKA_MARKETPLACE_TOPIC", DEFAULT_MARKETPLACE_TOPIC)
    bronze_paths = load_bronze_paths()
    starting_offsets = os.getenv(
        "SPARK_KAFKA_STARTING_OFFSETS", DEFAULT_STARTING_OFFSETS
    )
    master = os.getenv("SPARK_MASTER", "local[*]")

    spark = build_spark_session(master=master)
    spark.sparkContext.setLogLevel("WARN")
    try:
        source = read_marketplace_kafka_stream(
            spark,
            bootstrap_servers,
            topic,
            starting_offsets=starting_offsets,
        )
        frames = classify_marketplace_events(source)

        valid_writer = build_bronze_writer(
            frames.valid,
            output_path=bronze_paths.valid,
            checkpoint_path=bronze_paths.valid_checkpoint,
        )
        invalid_writer = build_bronze_writer(
            frames.invalid,
            output_path=bronze_paths.invalid,
            checkpoint_path=bronze_paths.invalid_checkpoint,
        )
        if args.continuous:
            valid_query = valid_writer.trigger(processingTime="5 seconds").start()
            invalid_query = invalid_writer.trigger(processingTime="5 seconds").start()
        else:
            valid_query = valid_writer.trigger(availableNow=True).start()
            invalid_query = invalid_writer.trigger(availableNow=True).start()

        valid_query.awaitTermination()
        invalid_query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping Spark streaming queries...")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
