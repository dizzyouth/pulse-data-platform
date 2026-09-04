"""Deterministic Spark tests for Gold marketplace analytics."""

from __future__ import annotations

import os
import sys
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from pyspark.sql import SparkSession

from src.analytics.gold_build import (
    DEFAULT_GOLD_CUSTOMER_METRICS_PATH,
    DEFAULT_GOLD_DAILY_SALES_PATH,
    DEFAULT_GOLD_FUNNEL_METRICS_PATH,
    DEFAULT_GOLD_PRODUCT_METRICS_PATH,
    DEFAULT_SILVER_SOURCE_PATH,
    GOLD_OUTPUT_CLEANUP_ATTEMPTS,
    GoldPaths,
    SILVER_VALID_SCHEMA,
    _prepare_gold_output,
    build_customer_metrics,
    build_daily_sales,
    build_funnel_metrics,
    build_gold_tables,
    build_product_metrics,
    load_gold_paths,
    validate_gold_tables,
    write_gold_tables,
)


def silver_event(
    event_id: str,
    event_type: str,
    *,
    timestamp: datetime | None = None,
    customer_id: str = "cus_1",
    country: str = "US",
    product_id: str | None = "prd_1",
    seller_id: str | None = "sel_1",
    order_id: str | None = None,
    payment_id: str | None = None,
    quantity: int | None = None,
    unit_price: float | None = None,
    currency: str = "USD",
    offset: int = 1,
):
    event_timestamp = timestamp or datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_timestamp": event_timestamp,
        "customer_id": customer_id,
        "session_id": f"ses_{customer_id}",
        "country": country,
        "product_id": product_id,
        "seller_id": seller_id,
        "order_id": order_id,
        "payment_id": payment_id,
        "quantity": quantity,
        "unit_price": unit_price,
        "currency": currency,
        "kafka_key": customer_id,
        "kafka_topic": "marketplace.events",
        "kafka_partition": 0,
        "kafka_offset": offset,
        "kafka_timestamp": event_timestamp,
        "ingested_at_utc": event_timestamp,
        "event_date": event_timestamp.date(),
    }


def realistic_events():
    return [
        silver_event("evt_view_1", "product_viewed", offset=1),
        silver_event("evt_view_2", "product_viewed", offset=2),
        silver_event("evt_cart", "product_added_to_cart", offset=3),
        silver_event("evt_checkout", "checkout_started", offset=4),
        silver_event(
            "evt_order",
            "order_created",
            order_id="ord_1",
            quantity=2,
            unit_price=10.0,
            offset=5,
        ),
        silver_event(
            "evt_payment",
            "payment_completed",
            order_id="ord_1",
            payment_id="pay_1",
            quantity=2,
            unit_price=10.0,
            offset=6,
        ),
        silver_event(
            "evt_delivered", "order_delivered", order_id="ord_1", offset=7
        ),
        silver_event(
            "evt_refund",
            "order_refunded",
            order_id="ord_1",
            payment_id="pay_1",
            quantity=2,
            unit_price=10.0,
            offset=8,
        ),
        silver_event(
            "evt_other_customer",
            "product_viewed",
            customer_id="cus_2",
            offset=9,
        ),
        silver_event(
            "evt_zero_denominator",
            "order_delivered",
            timestamp=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
            customer_id="cus_2",
            country="CA",
            order_id="ord_2",
            offset=10,
        ),
    ]


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS", "1") == "1", "Spark tests disabled")
class GoldAggregationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("pulse-gold-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.silver = cls.spark.createDataFrame(realistic_events(), SILVER_VALID_SCHEMA)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def frame(self, *events):
        return self.spark.createDataFrame(list(events), SILVER_VALID_SCHEMA)

    def test_daily_sales_calculations(self) -> None:
        row = build_daily_sales(self.silver).first()

        self.assertEqual(row.event_date, date(2026, 1, 1))
        self.assertEqual(row.completed_orders, 1)
        self.assertEqual(row.units_sold, 2)
        self.assertAlmostEqual(row.gross_revenue, 20.0)
        self.assertAlmostEqual(row.avg_order_value, 20.0)

    def test_completed_orders_are_distinct_and_revenue_is_payment_based(self) -> None:
        payments = self.frame(
            silver_event(
                "pay_1",
                "payment_completed",
                order_id="ord_same",
                quantity=2,
                unit_price=10.0,
            ),
            silver_event(
                "pay_2",
                "payment_completed",
                order_id="ord_same",
                quantity=1,
                unit_price=10.0,
                offset=2,
            ),
            silver_event(
                "refund",
                "order_refunded",
                order_id="ord_same",
                quantity=50,
                unit_price=10.0,
                offset=3,
            ),
        )

        row = build_daily_sales(payments).first()

        self.assertEqual(row.completed_orders, 1)
        self.assertEqual(row.units_sold, 3)
        self.assertAlmostEqual(row.gross_revenue, 30.0)
        self.assertAlmostEqual(row.avg_order_value, 30.0)

    def test_daily_sales_is_null_safe(self) -> None:
        row = build_daily_sales(
            self.frame(
                silver_event(
                    "pay_null",
                    "payment_completed",
                    order_id="ord_null",
                    quantity=None,
                    unit_price=None,
                )
            )
        ).first()

        self.assertEqual(row.completed_orders, 1)
        self.assertEqual(row.units_sold, 0)
        self.assertEqual(row.gross_revenue, 0.0)
        self.assertEqual(row.avg_order_value, 0.0)

    def test_customer_metrics_counts_and_revenue(self) -> None:
        row = build_customer_metrics(self.silver).filter("customer_id = 'cus_1'").first()

        self.assertEqual(row.products_viewed, 2)
        self.assertEqual(row.cart_adds, 1)
        self.assertEqual(row.checkouts_started, 1)
        self.assertEqual(row.orders_created, 1)
        self.assertEqual(row.payments_completed, 1)
        self.assertEqual(row.orders_delivered, 1)
        self.assertEqual(row.orders_refunded, 1)
        self.assertEqual(row.total_units_purchased, 2)
        self.assertEqual(row.total_revenue, 20.0)
        self.assertEqual(row.distinct_orders, 1)
        self.assertEqual(row.first_event_at, row.last_event_at)

    def test_product_metrics_counts_revenue_and_seller(self) -> None:
        row = build_product_metrics(self.silver).filter("product_id = 'prd_1'").first()

        self.assertEqual(row.seller_id, "sel_1")
        self.assertEqual(row.views, 3)
        self.assertEqual(row.cart_adds, 1)
        self.assertEqual(row.orders_created, 1)
        self.assertEqual(row.payments_completed, 1)
        self.assertEqual(row.units_sold, 2)
        self.assertEqual(row.gross_revenue, 20.0)
        self.assertEqual(row.distinct_customers, 2)

    def test_conflicting_product_sellers_produce_no_seller_id(self) -> None:
        product = self.frame(
            silver_event("one", "product_viewed", seller_id="seller_a"),
            silver_event("two", "product_viewed", seller_id="seller_b", offset=2),
        )

        self.assertIsNone(build_product_metrics(product).first().seller_id)

    def test_funnel_counts_rates_and_zero_denominators(self) -> None:
        funnel = build_funnel_metrics(self.silver)
        us = funnel.filter("event_date = DATE '2026-01-01' AND country = 'US'").first()
        ca = funnel.filter("event_date = DATE '2026-01-02' AND country = 'CA'").first()

        self.assertEqual(
            (
                us.product_views,
                us.cart_adds,
                us.checkouts_started,
                us.orders_created,
                us.payments_completed,
                us.orders_delivered,
                us.refunds,
            ),
            (3, 1, 1, 1, 1, 1, 1),
        )
        self.assertAlmostEqual(us.view_to_cart_rate, 1 / 3)
        self.assertEqual(us.cart_to_checkout_rate, 1.0)
        self.assertEqual(us.checkout_to_order_rate, 1.0)
        self.assertEqual(us.order_to_payment_rate, 1.0)
        self.assertIsNone(ca.view_to_cart_rate)
        self.assertIsNone(ca.cart_to_checkout_rate)
        self.assertIsNone(ca.checkout_to_order_rate)
        self.assertIsNone(ca.order_to_payment_rate)

    def test_gold_schemas_exclude_source_detail_columns(self) -> None:
        tables = build_gold_tables(self.silver)

        self.assertEqual(
            set(tables.daily_sales.columns),
            {
                "event_date",
                "country",
                "currency",
                "completed_orders",
                "units_sold",
                "gross_revenue",
                "avg_order_value",
            },
        )
        self.assertNotIn("raw_json", tables.customer_metrics.columns)
        self.assertNotIn("validation_errors", tables.product_metrics.columns)
        self.assertNotIn("silver_validation_errors", tables.funnel_metrics.columns)

    def test_sanity_validation_accepts_valid_aggregates_and_rejects_bad_rates(self) -> None:
        validate_gold_tables(build_gold_tables(self.silver))
        bad_funnel_source = self.frame(
            silver_event("view", "product_viewed"),
            silver_event("cart_1", "product_added_to_cart", offset=2),
            silver_event("cart_2", "product_added_to_cart", offset=3),
        )
        bad_tables = build_gold_tables(bad_funnel_source)

        with self.assertRaisesRegex(ValueError, "funnel_metrics"):
            validate_gold_tables(bad_tables)

    def test_gold_outputs_can_be_written_twice_to_the_same_paths(self) -> None:
        with TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            root = Path(directory)
            paths = GoldPaths(
                silver_source=root / "silver",
                daily_sales=root / "gold" / "daily_sales",
                customer_metrics=root / "gold" / "customer_metrics",
                product_metrics=root / "gold" / "product_metrics",
                funnel_metrics=root / "gold" / "funnel_metrics",
            )
            tables = build_gold_tables(self.silver)

            write_gold_tables(tables, paths)
            write_gold_tables(tables, paths)

            self.assertEqual(self.spark.read.parquet(str(paths.daily_sales)).count(), 1)
            self.assertEqual(
                self.spark.read.parquet(str(paths.customer_metrics)).count(), 2
            )
            self.assertEqual(
                self.spark.read.parquet(str(paths.product_metrics)).count(), 1
            )
            self.assertEqual(self.spark.read.parquet(str(paths.funnel_metrics)).count(), 2)


class GoldPathConfigurationTests(unittest.TestCase):
    def test_defaults_are_project_relative(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            paths = load_gold_paths({}, project_root=root)

            self.assertEqual(
                paths.silver_source, (root / DEFAULT_SILVER_SOURCE_PATH).resolve()
            )
            self.assertEqual(
                paths.daily_sales, (root / DEFAULT_GOLD_DAILY_SALES_PATH).resolve()
            )
            self.assertEqual(
                paths.customer_metrics,
                (root / DEFAULT_GOLD_CUSTOMER_METRICS_PATH).resolve(),
            )
            self.assertEqual(
                paths.product_metrics,
                (root / DEFAULT_GOLD_PRODUCT_METRICS_PATH).resolve(),
            )
            self.assertEqual(
                paths.funnel_metrics,
                (root / DEFAULT_GOLD_FUNNEL_METRICS_PATH).resolve(),
            )

    def test_source_and_outputs_are_configurable(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            paths = load_gold_paths(
                {
                    "SILVER_MARKETPLACE_VALID_PATH": "input/silver",
                    "GOLD_DAILY_SALES_PATH": "out/daily",
                    "GOLD_CUSTOMER_METRICS_PATH": "out/customer",
                    "GOLD_PRODUCT_METRICS_PATH": "out/product",
                    "GOLD_FUNNEL_METRICS_PATH": "out/funnel",
                },
                project_root=root,
            )

            self.assertEqual(paths.silver_source, (root / "input/silver").resolve())
            self.assertEqual(paths.daily_sales, (root / "out/daily").resolve())
            self.assertEqual(paths.customer_metrics, (root / "out/customer").resolve())
            self.assertEqual(paths.product_metrics, (root / "out/product").resolve())
            self.assertEqual(paths.funnel_metrics, (root / "out/funnel").resolve())

    def test_overlapping_source_and_output_paths_are_rejected(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)

            with self.assertRaisesRegex(ValueError, "cannot overlap"):
                load_gold_paths(
                    {"GOLD_DAILY_SALES_PATH": "data"}, project_root=root
                )

    def test_cleanup_retries_transient_windows_file_handle_failures(self) -> None:
        with TemporaryDirectory() as directory:
            output = Path(directory, "daily_sales")
            output.mkdir()

            with (
                patch(
                    "src.analytics.gold_build.shutil.rmtree",
                    side_effect=[PermissionError("busy"), PermissionError("busy"), None],
                ) as remove,
                patch("src.analytics.gold_build.time.sleep") as sleep,
            ):
                _prepare_gold_output(output, {output.resolve()})

            self.assertEqual(remove.call_count, 3)
            self.assertEqual(sleep.call_count, 2)

    def test_cleanup_reports_permanent_access_failure(self) -> None:
        with TemporaryDirectory() as directory:
            output = Path(directory, "daily_sales")
            output.mkdir()

            with (
                patch(
                    "src.analytics.gold_build.shutil.rmtree",
                    side_effect=PermissionError("access denied"),
                ) as remove,
                patch("src.analytics.gold_build.time.sleep"),
                self.assertRaisesRegex(
                    OSError,
                    rf"after {GOLD_OUTPUT_CLEANUP_ATTEMPTS} attempts",
                ) as raised,
            ):
                _prepare_gold_output(output, {output.resolve()})

            self.assertEqual(remove.call_count, GOLD_OUTPUT_CLEANUP_ATTEMPTS)
            self.assertIsInstance(raised.exception.__cause__, PermissionError)


if __name__ == "__main__":
    unittest.main()
