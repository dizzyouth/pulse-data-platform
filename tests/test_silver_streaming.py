"""Broker-free tests for Bronze-to-Silver marketplace curation."""

from __future__ import annotations

import json
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession

from src.producers.models import EventType
from src.streaming.silver_streaming import (
    BRONZE_VALID_SCHEMA,
    DEFAULT_BRONZE_SOURCE_PATH,
    DEFAULT_SILVER_REJECTED_CHECKPOINT_PATH,
    DEFAULT_SILVER_REJECTED_PATH,
    DEFAULT_SILVER_VALID_CHECKPOINT_PATH,
    DEFAULT_SILVER_VALID_PATH,
    SUPPORTED_EVENT_TYPES,
    build_silver_writer,
    classify_silver_events,
    load_silver_paths,
    read_bronze_valid_stream,
)


def bronze_record(**updates):
    record = {
        "event_id": " evt_1 ",
        "event_type": " PRODUCT_VIEWED ",
        "event_timestamp": datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        "customer_id": " cus_1 ",
        "session_id": " ses_1 ",
        "country": " us ",
        "product_id": " prd_1 ",
        "seller_id": " sel_1 ",
        "order_id": None,
        "payment_id": None,
        "quantity": 1,
        "unit_price": 12.5,
        "currency": " usd ",
        "kafka_key": "cus_1",
        "kafka_topic": "marketplace.events",
        "kafka_partition": 2,
        "kafka_offset": 17,
        "kafka_timestamp": datetime(2026, 1, 2, 3, tzinfo=timezone.utc),
        "raw_json": json.dumps({"event_id": "evt_1"}),
        "validation_errors": [],
        "ingested_at_utc": datetime(2026, 1, 2, 3, 1, tzinfo=timezone.utc),
        "ingestion_date": datetime(2026, 1, 2).date(),
    }
    return {**record, **updates}


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS", "1") == "1", "Spark tests disabled")
class SilverTransformationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("pulse-silver-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def classify(self, *records):
        frame = self.spark.createDataFrame(list(records), BRONZE_VALID_SCHEMA)
        return classify_silver_events(frame, deduplicate=False)

    def test_bronze_valid_schema_can_be_read(self) -> None:
        with TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            path = Path(directory) / "bronze"
            self.spark.createDataFrame(
                [bronze_record()], BRONZE_VALID_SCHEMA
            ).write.parquet(str(path))

            row = self.spark.read.schema(BRONZE_VALID_SCHEMA).parquet(str(path)).first()

            self.assertEqual(row.kafka_offset, 17)
            self.assertEqual(row.raw_json, '{"event_id": "evt_1"}')

    def test_normalizes_fields_and_derives_event_date(self) -> None:
        row = self.classify(bronze_record()).valid.first()

        self.assertEqual(row.event_id, "evt_1")
        self.assertEqual(row.event_type, "product_viewed")
        self.assertEqual(row.customer_id, "cus_1")
        self.assertEqual(row.product_id, "prd_1")
        self.assertEqual(row.country, "US")
        self.assertEqual(row.currency, "USD")
        self.assertEqual(row.quantity, 1)
        self.assertEqual(row.unit_price, 12.5)
        self.assertEqual(str(row.event_date), "2026-01-02")

    def test_supported_event_types_match_generator_contract(self) -> None:
        self.assertEqual(set(SUPPORTED_EVENT_TYPES), {item.value for item in EventType})
        for event_type in SUPPORTED_EVENT_TYPES:
            with self.subTest(event_type=event_type):
                row = self.classify(
                    bronze_record(event_id=f"evt_{event_type}", event_type=event_type)
                ).all_records.first()
                self.assertNotIn(
                    "unsupported_event_type", row.silver_validation_errors
                )

    def test_quality_rules_route_failures_to_rejected(self) -> None:
        cases = {
            "unsupported_event_type": {"event_type": "inventory_adjusted"},
            "invalid_quantity": {"quantity": 0},
            "invalid_unit_price": {"unit_price": -0.01},
            "invalid_country_code": {"country": "USA"},
            "invalid_currency_code": {"currency": "US"},
        }
        for expected_error, updates in cases.items():
            with self.subTest(expected_error=expected_error):
                row = self.classify(bronze_record(**updates)).rejected.first()
                self.assertIn(expected_error, row.silver_validation_errors)

    def test_valid_schema_preserves_lineage_but_omits_bronze_payload(self) -> None:
        columns = self.classify(bronze_record()).valid.columns

        self.assertTrue(
            {
                "kafka_key",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                "ingested_at_utc",
            }.issubset(columns)
        )
        self.assertNotIn("raw_json", columns)
        self.assertNotIn("validation_errors", columns)
        self.assertNotIn("silver_validation_errors", columns)

    def test_streaming_outputs_route_rejects_and_deduplicate_event_ids(self) -> None:
        with TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            root = Path(directory)
            bronze_path = root / "bronze"
            records = [
                bronze_record(),
                bronze_record(kafka_offset=18),
                bronze_record(
                    event_id="evt_bad",
                    quantity=0,
                    kafka_offset=19,
                    raw_json='{"event_id":"evt_bad","quantity":0}',
                ),
            ]
            self.spark.createDataFrame(records, BRONZE_VALID_SCHEMA).write.parquet(
                str(bronze_path)
            )
            source = read_bronze_valid_stream(self.spark, bronze_path)
            frames = classify_silver_events(source)
            paths = load_silver_paths(
                {"BRONZE_MARKETPLACE_VALID_PATH": str(bronze_path)},
                project_root=root,
            )

            valid_query = build_silver_writer(
                frames.valid,
                output_path=paths.valid,
                checkpoint_path=paths.valid_checkpoint,
            ).trigger(availableNow=True).start()
            rejected_query = build_silver_writer(
                frames.rejected,
                output_path=paths.rejected,
                checkpoint_path=paths.rejected_checkpoint,
            ).trigger(availableNow=True).start()
            valid_query.awaitTermination()
            rejected_query.awaitTermination()

            valid = self.spark.read.parquet(str(paths.valid))
            rejected = self.spark.read.parquet(str(paths.rejected))
            self.assertEqual(valid.count(), 1)
            self.assertEqual(valid.first().event_id, "evt_1")
            self.assertEqual(rejected.count(), 1)
            self.assertIn("invalid_quantity", rejected.first().silver_validation_errors)
            self.assertNotIn("raw_json", valid.columns)
            self.assertEqual(rejected.first().raw_json, records[-1]["raw_json"])
            self.assertTrue(list(paths.valid.glob("event_date=2026-01-02")))
            self.assertTrue(list(paths.rejected.glob("event_date=2026-01-02")))


class SilverPathConfigurationTests(unittest.TestCase):
    def test_defaults_are_project_relative_with_separate_checkpoints(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            paths = load_silver_paths({}, project_root=root)

            self.assertEqual(
                paths.bronze_source, (root / DEFAULT_BRONZE_SOURCE_PATH).resolve()
            )
            self.assertEqual(paths.valid, (root / DEFAULT_SILVER_VALID_PATH).resolve())
            self.assertEqual(
                paths.rejected, (root / DEFAULT_SILVER_REJECTED_PATH).resolve()
            )
            self.assertEqual(
                paths.valid_checkpoint,
                (root / DEFAULT_SILVER_VALID_CHECKPOINT_PATH).resolve(),
            )
            self.assertEqual(
                paths.rejected_checkpoint,
                (root / DEFAULT_SILVER_REJECTED_CHECKPOINT_PATH).resolve(),
            )
            self.assertNotEqual(paths.valid_checkpoint, paths.rejected_checkpoint)

    def test_all_paths_and_watermark_are_configurable(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            paths = load_silver_paths(
                {
                    "BRONZE_MARKETPLACE_VALID_PATH": "input/bronze",
                    "SILVER_MARKETPLACE_VALID_PATH": "output/good",
                    "SILVER_MARKETPLACE_REJECTED_PATH": "output/bad",
                    "SILVER_MARKETPLACE_VALID_CHECKPOINT_PATH": "state/good",
                    "SILVER_MARKETPLACE_REJECTED_CHECKPOINT_PATH": "state/bad",
                    "SILVER_EVENT_WATERMARK": "2 days",
                },
                project_root=root,
            )

            self.assertEqual(paths.bronze_source, (root / "input/bronze").resolve())
            self.assertEqual(paths.valid, (root / "output/good").resolve())
            self.assertEqual(paths.rejected, (root / "output/bad").resolve())
            self.assertEqual(paths.valid_checkpoint, (root / "state/good").resolve())
            self.assertEqual(
                paths.rejected_checkpoint, (root / "state/bad").resolve()
            )
            self.assertEqual(paths.event_watermark, "2 days")


if __name__ == "__main__":
    unittest.main()
