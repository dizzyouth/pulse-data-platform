"""Offline Shopify connector tests plus an explicitly opt-in read-only smoke test."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.onboarding.connector_state import ConnectorStateStore
from src.onboarding.models import BusinessConfig, SourceConfig
from src.onboarding.shopify import (
    HttpResponse, ShopifyAdapterError, ShopifyAdminApiAdapter,
)
from src.onboarding.shopify_ingestion import project_order, run_shopify_ingestion
from src.quality.models import Severity
from src.quality.shopify import check_shopify_order, duplicate_order_versions


TOKEN = "unit-test-secret-token"
BOUNDARY = datetime(2026, 1, 1, tzinfo=timezone.utc)
NOW = datetime(2026, 1, 8, 12, tzinfo=timezone.utc)


def money(amount: str, currency: str = "USD"):
    return {"shopMoney": {"amount": amount, "currencyCode": currency}}


def order_node(
    order_id="gid://shopify/Order/1", *, updated="2026-01-05T12:00:00Z",
    currency="USD", cancelled=False, refunded="0.00", product="gid://shopify/Product/1",
):
    refund_amount = float(refunded)
    refunds = [] if not refund_amount else [{
        "id": "gid://shopify/Refund/1", "createdAt": "2026-01-06T12:00:00Z",
        "totalRefundedSet": money(refunded, currency),
        "refundLineItems": {"nodes": [{
            "quantity": 1, "subtotalSet": money(refunded, currency),
            "lineItem": {"id": "gid://shopify/LineItem/1", "product": {"id": product},
                         "variant": {"id": "gid://shopify/ProductVariant/1"}, "sku": "SAFE-SKU"},
        }], "pageInfo": {"hasNextPage": False}},
    }]
    return {
        "id": order_id, "name": "#1001", "createdAt": "2026-01-04T23:30:00Z",
        "updatedAt": updated, "processedAt": "2026-01-04T23:35:00Z",
        "cancelledAt": "2026-01-07T00:00:00Z" if cancelled else None,
        "displayFinancialStatus": "PARTIALLY_REFUNDED" if refund_amount else "PAID",
        "displayFulfillmentStatus": "FULFILLED", "currencyCode": currency,
        "customer": {"id": "gid://shopify/Customer/1"},
        "shippingAddress": {"countryCodeV2": "US"},
        "totalPriceSet": money("100.00", currency),
        "subtotalPriceSet": money("90.00", currency),
        "totalDiscountsSet": money("5.00", currency),
        "totalTaxSet": money("10.00", currency),
        "totalShippingPriceSet": money("5.00", currency),
        "totalRefundedSet": money(refunded, currency),
        "lineItems": {"nodes": [{
            "id": "gid://shopify/LineItem/1", "product": {"id": product},
            "variant": {"id": "gid://shopify/ProductVariant/1"}, "sku": "SAFE-SKU",
            "quantity": 2, "originalUnitPriceSet": money("45.00", currency),
            "totalDiscountSet": money("5.00", currency),
        }], "pageInfo": {"hasNextPage": False}},
        "refunds": refunds,
        # These simulate accidental response additions; normalize must discard them.
        "email": "never.persist@example.com", "phone": "+15555550100",
    }


def page(nodes, *, next_page=False, cursor=None):
    return {"data": {"orders": {"nodes": nodes, "pageInfo": {
        "hasNextPage": next_page, "endCursor": cursor,
    }}}}


def response(payload, status=200, headers=None):
    return HttpResponse(status=status, headers=headers or {}, body=json.dumps(payload).encode())


def config():
    return SourceConfig(
        source_type="shopify", source_id="shopify_orders", enabled=True,
        business_id="business_a", ingestion_mode="batch", schedule="0 * * * *",
        schema_version="shopify_orders_v1", credential_ref="SHOPIFY_TEST_TOKEN",
        metadata={
            "adapter": "admin_api", "shop_domain_ref": "SHOPIFY_TEST_DOMAIN",
            "backfill_start_ref": "SHOPIFY_TEST_START", "api_version": "2026-07",
            "page_size": 1, "timeout_seconds": 2, "max_retries": 2,
            "backoff_seconds": 0.01,
        },
    )


def business():
    return BusinessConfig(
        business_id="business_a", business_name="Business A", status="ACTIVE",
        timezone="America/New_York", currency="USD", country="US",
        enabled_sources=("shopify_orders",), reporting_timezone="America/New_York",
    )


class QueueTransport:
    def __init__(self, items):
        self.items = list(items)
        self.calls = []

    def __call__(self, url, headers, body, timeout):
        self.calls.append((url, dict(headers), json.loads(body), timeout))
        item = self.items.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


class MemoryPersister:
    def __init__(self, fail=False):
        self.rows = []
        self.fail = fail

    def persist(self, rows):
        if self.fail:
            raise RuntimeError("persistence failed")
        self.rows.extend(rows)


class ShopifyAdapterTests(unittest.TestCase):
    def adapter(self, transport, sleeps=None):
        return ShopifyAdminApiAdapter(
            config(), environ={
                "SHOPIFY_TEST_TOKEN": TOKEN,
                "SHOPIFY_TEST_DOMAIN": "example.myshopify.com",
                "SHOPIFY_TEST_START": "2026-01-01T00:00:00Z",
            }, transport=transport, sleep=(sleeps if sleeps is not None else []).append,
            now=lambda: NOW,
        )

    def test_complete_order_pagination_and_stable_version_identity(self):
        transport = QueueTransport([
            response(page([order_node()], next_page=True, cursor="cursor-1")),
            response(page([order_node("gid://shopify/Order/2")], next_page=False)),
        ])
        records = self.adapter(transport).extract(backfill_start=BOUNDARY)
        self.assertEqual(len(records), 2)
        self.assertIsNone(transport.calls[0][2]["variables"]["after"])
        self.assertEqual(transport.calls[1][2]["variables"]["after"], "cursor-1")
        self.assertIn("updated_at:>=", transport.calls[0][2]["variables"]["query"])
        self.assertTrue(all(call[0].startswith("https://") for call in transport.calls))
        self.assertTrue(all(call[1]["X-Shopify-Access-Token"] == TOKEN for call in transport.calls))

        again = self.adapter(QueueTransport([response(page([order_node()]))])).extract(
            watermark=BOUNDARY
        )[0]
        self.assertEqual(records[0].record_id, again.record_id)
        self.assertNotEqual(records[0].ingestion_id, again.ingestion_id)

    def test_normalization_is_minimal_and_captures_refunds_and_lines(self):
        adapter = self.adapter(QueueTransport([response(page([order_node(refunded="25.00")]))]))
        normalized = adapter.normalize(adapter.extract(backfill_start=BOUNDARY)[0])
        self.assertEqual(normalized["refunded_amount"], 25.0)
        self.assertEqual(normalized["line_items"][0]["variant_id"], "gid://shopify/ProductVariant/1")
        self.assertEqual(normalized["line_items"][0]["discount_amount"], 5.0)
        self.assertEqual(normalized["refunds"][0]["amount"], 25.0)
        serialized = json.dumps(normalized)
        self.assertNotIn("never.persist@example.com", serialized)
        self.assertNotIn("+15555550100", serialized)

    def test_rate_limit_retries_are_bounded(self):
        sleeps = []
        transport = QueueTransport([
            response({}, status=429, headers={"Retry-After": "0.02"}),
            response(page([order_node()])),
        ])
        records = self.adapter(transport, sleeps).extract(backfill_start=BOUNDARY)
        self.assertEqual(len(records), 1)
        self.assertEqual(len(transport.calls), 2)
        self.assertEqual(sleeps, [0.02])

    def test_auth_timeout_and_malformed_errors_are_sanitized(self):
        cases = [
            (QueueTransport([response({}, status=401)]), "authentication"),
            (QueueTransport([TimeoutError("timeout with " + TOKEN)] * 3), "timed out"),
            (QueueTransport([HttpResponse(200, {}, b"not-json")]), "malformed JSON"),
        ]
        for transport, expected in cases:
            with self.subTest(expected=expected):
                with self.assertRaises(ShopifyAdapterError) as caught:
                    self.adapter(transport).extract(backfill_start=BOUNDARY)
                self.assertIn(expected, str(caught.exception))
                self.assertNotIn(TOKEN, str(caught.exception))

    def test_health_distinguishes_permission_and_success(self):
        denied = self.adapter(QueueTransport([response({"data": {
            "shop": {"id": "gid://shopify/Shop/1"},
            "currentAppInstallation": {"accessScopes": [{"handle": "read_products"}]},
        }})])).healthcheck()
        self.assertEqual(denied.status, "permission_failure")
        self.assertFalse(denied.permission_granted)
        healthy = self.adapter(QueueTransport([response({"data": {
            "shop": {"id": "gid://shopify/Shop/1"},
            "currentAppInstallation": {"accessScopes": [{"handle": "read_orders"}]},
        }})])).healthcheck()
        self.assertTrue(healthy.healthy)
        self.assertTrue(healthy.authenticated)

    def test_nested_line_item_pagination_is_complete(self):
        node = order_node()
        node["lineItems"]["pageInfo"] = {"hasNextPage": True, "endCursor": "line-cursor"}
        second_line = dict(node["lineItems"]["nodes"][0], id="gid://shopify/LineItem/2")
        transport = QueueTransport([
            response(page([node])),
            response({"data": {"order": {"lineItems": {
                "nodes": [second_line], "pageInfo": {"hasNextPage": False, "endCursor": None},
            }}}}),
        ])
        adapter = self.adapter(transport)
        normalized = adapter.normalize(adapter.extract(backfill_start=BOUNDARY)[0])
        self.assertEqual(len(normalized["line_items"]), 2)
        self.assertEqual(transport.calls[1][2]["variables"]["after"], "line-cursor")


class ShopifyIngestionTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = ConnectorStateStore(Path(self.temp.name) / "state.sqlite3")
        self.environment = {
            "SHOPIFY_TEST_TOKEN": TOKEN, "SHOPIFY_TEST_DOMAIN": "example.myshopify.com",
            "SHOPIFY_TEST_START": "2026-01-01T00:00:00Z",
        }

    def adapter(self, *nodes):
        return ShopifyAdminApiAdapter(
            config(), environ=self.environment,
            transport=QueueTransport([response(page(list(nodes)))]), now=lambda: NOW,
        )

    def test_checkpoint_advances_only_after_successful_bronze_persistence(self):
        persister = MemoryPersister()
        report = run_shopify_ingestion(
            config(), business(), adapter=self.adapter(order_node()), state_store=self.store,
            persister=persister, environ=self.environment, now=lambda: NOW,
        )
        self.assertEqual(report.mode, "backfill")
        self.assertEqual(self.store.get("business_a", "shopify_orders").checkpoint_utc,
                         datetime(2026, 1, 5, 12, tzinfo=timezone.utc))
        self.assertTrue(persister.rows)

        prior = self.store.get("business_a", "shopify_orders").checkpoint_utc
        with self.assertRaisesRegex(RuntimeError, "persistence failed"):
            run_shopify_ingestion(
                config(), business(), adapter=self.adapter(order_node(updated="2026-01-07T12:00:00Z")),
                state_store=self.store, persister=MemoryPersister(fail=True),
                environ=self.environment, now=lambda: NOW,
            )
        state = self.store.get("business_a", "shopify_orders")
        self.assertEqual(state.checkpoint_utc, prior)
        self.assertEqual(state.health, "unhealthy")

    def test_dry_run_has_no_persistence_or_state_mutation(self):
        persister = MemoryPersister()
        report = run_shopify_ingestion(
            config(), business(), adapter=self.adapter(order_node()), state_store=self.store,
            persister=persister, environ=self.environment, limit=1, dry_run=True,
        )
        self.assertFalse(report.persisted)
        self.assertEqual(len(report.sample), 1)
        self.assertFalse(persister.rows)
        self.assertIsNone(self.store.get("business_a", "shopify_orders").checkpoint_utc)

    def test_refunds_cancellations_currency_timezone_and_removed_line_tombstone(self):
        adapter = self.adapter(order_node(refunded="25.00", currency="CAD"))
        envelope = adapter.extract(backfill_start=BOUNDARY)[0]
        normalized = adapter.normalize(envelope)
        rows = project_order(config(), business(), envelope, normalized)
        payment = next(row for row in rows if row["event_type"] == "payment_completed")
        refund = next(row for row in rows if row["event_type"] == "order_refunded")
        self.assertEqual(payment["event_amount"], 75.0)
        self.assertEqual(payment["currency"], "CAD")
        self.assertEqual(refund["event_amount"], 25.0)
        self.assertEqual(str(payment["reporting_date"]), "2026-01-04")

        cancelled_adapter = self.adapter(order_node(cancelled=True))
        cancelled_envelope = cancelled_adapter.extract(backfill_start=BOUNDARY)[0]
        cancelled = project_order(
            config(), business(), cancelled_envelope,
            cancelled_adapter.normalize(cancelled_envelope),
        )
        self.assertTrue(all(not row["is_active"] for row in cancelled))

        first = MemoryPersister()
        run_shopify_ingestion(
            config(), business(), adapter=self.adapter(order_node()), state_store=self.store,
            persister=first, environ=self.environment,
        )
        edited = order_node(updated="2026-01-07T12:00:00Z", product=None)
        edited["lineItems"]["nodes"] = []
        second = MemoryPersister()
        run_shopify_ingestion(
            config(), business(), adapter=self.adapter(edited), state_store=self.store,
            persister=second, environ=self.environment,
        )
        self.assertTrue(any(not row["is_active"] for row in second.rows))

    def test_full_refund_duplicate_and_quality_classification(self):
        adapter = self.adapter(order_node(refunded="100.00"))
        envelope = adapter.extract(backfill_start=BOUNDARY)[0]
        normalized = adapter.normalize(envelope)
        payments = [row for row in project_order(
            config(), business(), envelope, normalized
        ) if row["event_type"] == "payment_completed"]
        self.assertEqual(sum(row["event_amount"] for row in payments), 0.0)
        self.assertEqual(check_shopify_order(normalized), ())
        duplicates = duplicate_order_versions([normalized, normalized])
        self.assertEqual(duplicates[0].severity, Severity.CRITICAL)
        invalid = dict(normalized, currency="usd")
        self.assertIn("invalid_currency", {item.code for item in check_shopify_order(invalid)})


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS", "1") == "1", "Spark tests disabled")
class ShopifySparkPathTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src.analytics.gold_build import build_gold_spark_session

        cls.spark = build_gold_spark_session(
            app_name="pulse-shopify-path-tests", master="local[1]"
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_real_projection_uses_latest_update_and_never_builds_funnel(self):
        from src.analytics.gold_build import build_gold_tables
        from src.streaming.silver_streaming import (
            BRONZE_VALID_SCHEMA, SilverPaths, build_silver_snapshot,
        )

        older_adapter = ShopifyAdminApiAdapter(
            config(), environ={"SHOPIFY_TEST_TOKEN": TOKEN, "SHOPIFY_TEST_DOMAIN": "example.myshopify.com"},
            transport=QueueTransport([response(page([order_node()]))]), now=lambda: NOW,
        )
        older_envelope = older_adapter.extract(backfill_start=BOUNDARY)[0]
        older = project_order(
            config(), business(), older_envelope, older_adapter.normalize(older_envelope)
        )
        newer_adapter = ShopifyAdminApiAdapter(
            config(), environ={"SHOPIFY_TEST_TOKEN": TOKEN, "SHOPIFY_TEST_DOMAIN": "example.myshopify.com"},
            transport=QueueTransport([response(page([
                order_node(updated="2026-01-07T12:00:00Z", refunded="25.00")
            ]))]), now=lambda: NOW,
        )
        newer_envelope = newer_adapter.extract(backfill_start=BOUNDARY)[0]
        newer = project_order(
            config(), business(), newer_envelope, newer_adapter.normalize(newer_envelope)
        )
        with TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            root = Path(directory)
            bronze = root / "bronze"
            valid = root / "silver" / "valid"
            rejected = root / "silver" / "rejected"
            self.spark.createDataFrame(older + newer, BRONZE_VALID_SCHEMA).write.parquet(str(bronze))
            build_silver_snapshot(self.spark, SilverPaths(
                bronze_source=bronze, valid=valid, rejected=rejected,
                valid_checkpoint=root / "check-valid", rejected_checkpoint=root / "check-rejected",
                event_watermark="7 days",
            ))
            silver = self.spark.read.parquet(str(valid))
            tables = build_gold_tables(silver)
            sales = tables.daily_sales.first()
            self.assertAlmostEqual(sales.gross_revenue, 75.0)
            self.assertEqual(str(sales.event_date), "2026-01-04")
            self.assertEqual(tables.funnel_metrics.count(), 0)


@unittest.skipUnless(
    os.environ.get("RUN_SHOPIFY_INTEGRATION_TESTS") == "1",
    "real read-only Shopify tests are opt-in",
)
class RealShopifyReadOnlyTests(unittest.TestCase):
    def test_health_and_small_order_page(self):
        required = (
            "SHOPIFY_INTEGRATION_SHOP_DOMAIN", "SHOPIFY_INTEGRATION_ACCESS_TOKEN",
            "SHOPIFY_INTEGRATION_BACKFILL_START_DATE",
        )
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            self.skipTest("missing opt-in Shopify environment variables")
        source = SourceConfig(
            source_type="shopify", source_id="integration_shopify", enabled=True,
            business_id="integration_shopify", ingestion_mode="batch", schedule="0 * * * *",
            schema_version="shopify_orders_v1",
            credential_ref="SHOPIFY_INTEGRATION_ACCESS_TOKEN",
            metadata={"adapter": "admin_api", "shop_domain_ref": "SHOPIFY_INTEGRATION_SHOP_DOMAIN",
                      "backfill_start_ref": "SHOPIFY_INTEGRATION_BACKFILL_START_DATE",
                      "api_version": "2026-07", "page_size": 2},
        )
        adapter = ShopifyAdminApiAdapter(source)
        self.assertTrue(adapter.healthcheck().healthy)
        start = datetime.fromisoformat(
            os.environ["SHOPIFY_INTEGRATION_BACKFILL_START_DATE"].replace("Z", "+00:00")
        )
        for envelope in adapter.extract(backfill_start=start, limit=2):
            normalized = adapter.normalize(envelope)
            self.assertTrue(normalized["order_id"])
            self.assertTrue(normalized["updated_at_utc"])


if __name__ == "__main__":
    unittest.main()
