"""Focused Spark tests for marketplace stream parsing and classification."""

from __future__ import annotations

import json
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.streaming.spark_streaming import (
    DEFAULT_BRONZE_INVALID_CHECKPOINT_PATH,
    DEFAULT_BRONZE_INVALID_PATH,
    DEFAULT_BRONZE_VALID_CHECKPOINT_PATH,
    DEFAULT_BRONZE_VALID_PATH,
    MARKETPLACE_EVENT_SCHEMA,
    build_bronze_writer,
    classify_marketplace_events,
    load_bronze_paths,
)
from src.streaming.windows_spark import (
    configure_windows_spark_builder,
    configure_windows_spark_environment,
)

KAFKA_TEST_SCHEMA = StructType(
    [
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", LongType(), False),
        StructField("timestamp", TimestampType(), False),
    ]
)

VALID_EVENT = {
    "event_id": "evt_1",
    "event_type": "product_viewed",
    "event_timestamp": "2026-01-01T00:00:00Z",
    "customer_id": "cus_1",
    "session_id": "ses_1",
    "country": "US",
    "product_id": "prd_1",
    "seller_id": "sel_1",
    "unit_price": 12.5,
    "currency": "USD",
}


class WindowsSparkEnvironmentTests(unittest.TestCase):
    def test_configures_project_local_process_environment(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            environment: dict[str, str] = {}

            # Construct the host-native Path before simulating Windows: os is
            # shared with pathlib, which otherwise selects WindowsPath on Linux.
            with patch("src.streaming.windows_spark.os.name", "nt"):
                configure_windows_spark_environment(
                    project_root=root, environ=environment
                )

            self.assertEqual(environment["HADOOP_HOME"], str(root / "tmp" / "hadoop"))
            self.assertEqual(environment["TEMP"], str(root / "tmp" / "spark"))
            self.assertEqual(environment["TMP"], str(root / "tmp" / "spark"))
            self.assertEqual(
                environment["PATH"].split(os.pathsep)[0],
                str(root / "tmp" / "hadoop" / "bin"),
            )
            self.assertTrue((root / "tmp" / "spark").is_dir())

    def test_preserves_explicit_environment_overrides(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "HADOOP_HOME": "custom-hadoop",
                "TEMP": "custom-temp",
                "TMP": "custom-tmp",
                "PATH": "custom-path",
            }

            with patch("src.streaming.windows_spark.os.name", "nt"):
                configure_windows_spark_environment(
                    project_root=root, environ=environment
                )

            self.assertEqual(environment["HADOOP_HOME"], "custom-hadoop")
            self.assertEqual(environment["TEMP"], "custom-temp")
            self.assertEqual(environment["TMP"], "custom-tmp")
            self.assertEqual(
                environment["PATH"],
                os.pathsep.join(
                    (str(root / "tmp" / "hadoop" / "bin"), "custom-path")
                ),
            )

    def test_non_windows_helpers_leave_environment_and_builder_untouched(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {"PATH": "original-path", "TEMP": "original-temp"}
            original = environment.copy()
            builder = object()

            with patch("src.streaming.windows_spark.os.name", "posix"):
                configure_windows_spark_environment(
                    project_root=root, environ=environment
                )
                configured = configure_windows_spark_builder(builder, project_root=root)

            self.assertEqual(environment, original)
            self.assertIs(configured, builder)
            self.assertEqual(list(root.iterdir()), [])


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS", "1") == "1", "Spark tests disabled")
class SparkStreamingTransformationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("pulse-spark-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def record(self, payload, *, key=b"cus_1", partition=2, offset=17):
        value = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        return (
            key,
            value,
            "marketplace.events",
            partition,
            offset,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    def classify(self, *records):
        dataframe = self.spark.createDataFrame(list(records), KAFKA_TEST_SCHEMA)
        return classify_marketplace_events(dataframe)

    def test_explicit_schema_contains_required_and_optional_fields(self) -> None:
        names = set(MARKETPLACE_EVENT_SCHEMA.fieldNames())
        self.assertTrue(
            {
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
            }.issubset(names)
        )

    def test_valid_json_is_parsed_and_preserves_kafka_metadata(self) -> None:
        row = self.classify(self.record(VALID_EVENT)).valid.first()

        self.assertEqual(row.event_id, "evt_1")
        self.assertEqual(row.customer_id, "cus_1")
        self.assertEqual(row.kafka_key, "cus_1")
        self.assertEqual(row.kafka_topic, "marketplace.events")
        self.assertEqual(row.kafka_partition, 2)
        self.assertEqual(row.kafka_offset, 17)
        expected_local_timestamp = (
            datetime(2026, 1, 1, tzinfo=timezone.utc)
            .astimezone()
            .replace(tzinfo=None)
        )
        self.assertEqual(row.kafka_timestamp, expected_local_timestamp)
        self.assertEqual(row.raw_json, json.dumps(VALID_EVENT))
        self.assertIsNotNone(row.ingested_at_utc)
        self.assertEqual(
            row.ingestion_date,
            row.ingested_at_utc.astimezone(timezone.utc).date(),
        )

    def test_bronze_schema_contains_lineage_and_ingestion_columns(self) -> None:
        fields = set(self.classify(self.record(VALID_EVENT)).all_records.columns)
        self.assertTrue(
            {
                "raw_json",
                "kafka_key",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                "ingested_at_utc",
                "ingestion_date",
            }.issubset(fields)
        )

    def test_malformed_json_is_invalid(self) -> None:
        row = self.classify(self.record(b"{not-json")).invalid.first()
        self.assertEqual(row.validation_errors, ["malformed_json"])
        self.assertEqual(row.raw_json, "{not-json")
        self.assertEqual(row.kafka_partition, 2)
        self.assertEqual(row.kafka_offset, 17)

    def test_each_required_field_is_validated(self) -> None:
        required = ("event_id", "event_type", "event_timestamp", "customer_id")
        for field in required:
            with self.subTest(field=field):
                payload = {key: value for key, value in VALID_EVENT.items() if key != field}
                row = self.classify(self.record(payload)).invalid.first()
                self.assertIn(f"missing_{field}", row.validation_errors)

    def test_key_customer_id_mismatch_is_invalid(self) -> None:
        row = self.classify(self.record(VALID_EVENT, key=b"cus_other")).invalid.first()
        self.assertIn("kafka_key_customer_id_mismatch", row.validation_errors)

    def test_streaming_writers_persist_valid_and_invalid_parquet(self) -> None:
        with TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            root = Path(directory)
            input_path = root / "input"
            records = [self.record(VALID_EVENT), self.record(b"{not-json", offset=18)]
            self.spark.createDataFrame(records, KAFKA_TEST_SCHEMA).write.parquet(
                str(input_path)
            )
            source = self.spark.readStream.schema(KAFKA_TEST_SCHEMA).parquet(
                str(input_path)
            )
            frames = classify_marketplace_events(source)
            paths = load_bronze_paths({}, project_root=root)

            valid_query = build_bronze_writer(
                frames.valid,
                output_path=paths.valid,
                checkpoint_path=paths.valid_checkpoint,
            ).trigger(availableNow=True).start()
            invalid_query = build_bronze_writer(
                frames.invalid,
                output_path=paths.invalid,
                checkpoint_path=paths.invalid_checkpoint,
            ).trigger(availableNow=True).start()
            valid_query.awaitTermination()
            invalid_query.awaitTermination()

            valid_row = self.spark.read.parquet(str(paths.valid)).first()
            invalid_row = self.spark.read.parquet(str(paths.invalid)).first()
            self.assertEqual(valid_row.event_id, "evt_1")
            self.assertEqual(valid_row.kafka_offset, 17)
            self.assertEqual(invalid_row.raw_json, "{not-json")
            self.assertEqual(invalid_row.validation_errors, ["malformed_json"])
            self.assertTrue(list(paths.valid.glob("ingestion_date=*")))
            self.assertTrue(list(paths.invalid.glob("ingestion_date=*")))


class BronzePathConfigurationTests(unittest.TestCase):
    def test_default_paths_are_resolved_relative_to_project_root(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            paths = load_bronze_paths({}, project_root=root)

            self.assertEqual(paths.valid, (root / DEFAULT_BRONZE_VALID_PATH).resolve())
            self.assertEqual(
                paths.invalid, (root / DEFAULT_BRONZE_INVALID_PATH).resolve()
            )
            self.assertEqual(
                paths.valid_checkpoint,
                (root / DEFAULT_BRONZE_VALID_CHECKPOINT_PATH).resolve(),
            )
            self.assertEqual(
                paths.invalid_checkpoint,
                (root / DEFAULT_BRONZE_INVALID_CHECKPOINT_PATH).resolve(),
            )
            self.assertNotEqual(paths.valid_checkpoint, paths.invalid_checkpoint)

    def test_paths_are_independently_configurable(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            paths = load_bronze_paths(
                {
                    "BRONZE_MARKETPLACE_VALID_PATH": "custom/good",
                    "BRONZE_MARKETPLACE_INVALID_PATH": "custom/bad",
                    "BRONZE_MARKETPLACE_VALID_CHECKPOINT_PATH": "state/good",
                    "BRONZE_MARKETPLACE_INVALID_CHECKPOINT_PATH": "state/bad",
                },
                project_root=root,
            )

            self.assertEqual(paths.valid, (root / "custom/good").resolve())
            self.assertEqual(paths.invalid, (root / "custom/bad").resolve())
            self.assertEqual(paths.valid_checkpoint, (root / "state/good").resolve())
            self.assertEqual(
                paths.invalid_checkpoint, (root / "state/bad").resolve()
            )


if __name__ == "__main__":
    unittest.main()
