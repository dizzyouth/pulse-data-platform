"""Deterministic Phase 5.9 registry, contract, adapter, and CLI tests."""

from __future__ import annotations

from contextlib import redirect_stdout
from datetime import datetime, timezone
from io import StringIO
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.onboarding.adapters import MockMetaAdsAdapter, MockShopifyAdapter, adapter_for
from src.onboarding.cli import main
from src.onboarding.contracts import SourceContractError, contract_for
from src.onboarding.credentials import CredentialResolutionError, resolve_credential
from src.onboarding.demo import run_demo
from src.onboarding.models import IngestionEnvelope, SourceConfig
from src.onboarding.registry import BusinessRegistry, validate_business
from src.orchestration.business_sources import discover_enabled_sources


ROOT = Path(__file__).resolve().parents[1]


class SampleRegistryTests(unittest.TestCase):
    def setUp(self):
        self.registry = BusinessRegistry(
            ROOT / "config" / "businesses", ROOT / "config" / "sources"
        )

    def test_sample_business_and_source_registry_are_valid(self):
        business = self.registry.get_business("pulse_demo_store")
        report = validate_business(self.registry, business.business_id)
        self.assertTrue(report.valid, report.to_dict())
        self.assertEqual(report.source_count, 3)
        self.assertEqual({item.source_type for item in self.registry.sources_for(business.business_id)},
                         {"shopify", "meta_ads", "csv_manual"})

    def test_schema_versions_are_explicit_and_unsupported_versions_fail(self):
        self.assertEqual(contract_for("shopify", "shopify_orders_v1").unique_grain,
                         ("order_id",))
        with self.assertRaisesRegex(SourceContractError, "Unsupported schema version"):
            contract_for("shopify", "shopify_orders_v2")
        with self.assertRaisesRegex(SourceContractError, "Missing required field"):
            contract_for("meta_ads", "meta_ads_daily_v1").validate({})

    def test_mock_adapters_are_deterministic_and_business_scoped(self):
        configs = {item.source_type: item for item in self.registry.sources_for("pulse_demo_store")}
        for source_type, adapter_type in (("shopify", MockShopifyAdapter),
                                          ("meta_ads", MockMetaAdsAdapter)):
            adapter = adapter_for(configs[source_type])
            self.assertIsInstance(adapter, adapter_type)
            first, second = adapter.extract(), adapter.extract()
            self.assertEqual(first, second)
            self.assertTrue(all(record.business_id == "pulse_demo_store" for record in first))
            self.assertTrue(all(adapter.normalize(record)["source_id"] == configs[source_type].source_id
                                for record in first))

    def test_credential_reference_resolves_only_at_runtime(self):
        config = next(item for item in self.registry.sources_for("pulse_demo_store")
                      if item.source_type == "shopify")
        self.assertEqual(resolve_credential(config, {config.credential_ref: "runtime-secret"}),
                         "runtime-secret")
        with self.assertRaisesRegex(CredentialResolutionError, config.credential_ref) as error:
            resolve_credential(config, {})
        self.assertNotIn("runtime-secret", str(error.exception))

    def test_ingestion_envelope_requires_stable_identity_and_utc_lineage(self):
        envelope = IngestionEnvelope(
            business_id="business_a", source_type="shopify", source_id="orders_main",
            ingestion_id="ing_1", record_id="rec_1",
            extracted_at_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
            schema_version="shopify_orders_v1", payload={"order_id": "ord_1"},
        )
        self.assertEqual(envelope.to_dict()["business_id"], "business_a")
        with self.assertRaisesRegex(ValueError, "business_id"):
            IngestionEnvelope(
                business_id="Business A", source_type="shopify", source_id="orders_main",
                ingestion_id="ing", record_id="rec",
                extracted_at_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
                schema_version="shopify_orders_v1", payload={},
            )

    def test_synthetic_demo_proves_all_sources_without_network(self):
        report = run_demo(registry=self.registry)
        self.assertEqual(report.bronze_records, 6)
        self.assertEqual(report.silver_records, 6)
        self.assertEqual(report.shopify_orders, 2)
        self.assertEqual(report.shopify_revenue, 200.0)
        self.assertEqual(report.meta_ads_spend, 55.0)
        self.assertEqual(report.warehouse_rows, 4)
        self.assertEqual(report.queried_business_rows, 4)
        self.assertEqual(report.quality_status, "PASS")
        self.assertEqual(report.anomaly_status, "INSUFFICIENT_HISTORY")

    def test_discovery_is_config_driven_and_stable(self):
        descriptors = discover_enabled_sources(self.registry)
        self.assertEqual([item.source_id for item in descriptors],
                         ["demo_csv", "demo_meta_ads", "demo_shopify"])
        self.assertEqual(len({item.task_id for item in descriptors}), 3)
        self.assertTrue(all(item.schema_version.endswith("_v1") for item in descriptors))

    def test_multi_business_orders_and_ads_fixture_uses_overlapping_ids_safely(self):
        fixture = json.loads((ROOT / "tests" / "fixtures" / "onboarding" /
                              "multi_business.json").read_text(encoding="utf-8"))
        records = []
        for item in fixture["sources"]:
            config = SourceConfig(
                **item, enabled=True, ingestion_mode="batch", schedule="0 * * * *",
                credential_ref="SYNTHETIC_TOKEN", metadata={"synthetic": True},
            )
            records.extend(adapter_for(config).extract())
        self.assertEqual({record.business_id for record in records},
                         set(fixture["businesses"]))
        for identifier in fixture["overlapping_identifiers"].values():
            owners = {record.business_id for record in records
                      if identifier in record.payload.values()}
            self.assertEqual(owners, set(fixture["businesses"]))
        self.assertEqual(len({(record.business_id, record.source_id, record.record_id)
                              for record in records}), len(records))

    def test_cli_list_show_validate_and_source_check(self):
        environment = {
            "PULSE_BUSINESSES_DIR": str(ROOT / "config" / "businesses"),
            "PULSE_SOURCES_DIR": str(ROOT / "config" / "sources"),
        }
        for args in (("list",), ("show", "pulse_demo_store"),
                     ("validate", "pulse_demo_store"),
                     ("source-check", "pulse_demo_store", "demo_shopify")):
            output = StringIO()
            with patch.dict(os.environ, environment), redirect_stdout(output):
                self.assertEqual(main(args), 0)
            self.assertTrue(json.loads(output.getvalue()))


class IsolatedRegistryTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.businesses, self.sources = root / "businesses", root / "sources"
        self.businesses.mkdir()
        self.sources.mkdir()

    def write(self, path, value):
        path.write_text(json.dumps(value), encoding="utf-8")

    def business(self, business_id, enabled=("orders",)):
        self.write(self.businesses / f"{business_id}.json", {
            "business_id": business_id, "business_name": business_id,
            "status": "ACTIVE", "timezone": "UTC", "currency": "USD",
            "country": "US", "enabled_sources": list(enabled),
            "reporting_timezone": "UTC", "metadata": {"synthetic": True},
        })

    def source(self, filename, business_id, *, credential="BUSINESS_TOKEN",
               version="shopify_orders_v1"):
        self.write(self.sources / filename, {
            "source_type": "shopify", "source_id": "orders", "enabled": True,
            "business_id": business_id, "ingestion_mode": "batch",
            "schedule": "0 * * * *", "schema_version": version,
            "credential_ref": credential, "metadata": {"synthetic": True},
        })

    def registry(self):
        with patch.dict(os.environ, {"PULSE_BUSINESSES_DIR": "", "PULSE_SOURCES_DIR": ""}, clear=False):
            return BusinessRegistry(self.businesses, self.sources)

    def test_source_ids_can_repeat_across_businesses(self):
        self.business("business_a")
        self.business("business_b")
        self.source("a.json", "business_a")
        self.source("b.json", "business_b")
        registry = BusinessRegistry(self.businesses, self.sources)
        self.assertTrue(validate_business(registry, "business_a").valid)
        self.assertTrue(validate_business(registry, "business_b").valid)
        self.assertEqual(len(registry.list_sources()), 2)

    def test_missing_credential_and_unsupported_version_are_structured(self):
        self.business("business_a")
        self.source("a.json", "business_a", credential="", version="shopify_orders_v9")
        report = validate_business(BusinessRegistry(self.businesses, self.sources), "business_a")
        self.assertFalse(report.valid)
        self.assertEqual({issue.code for issue in report.issues},
                         {"missing_credential_ref", "unsupported_schema_version"})

    def test_missing_source_and_bad_business_metadata_are_reported(self):
        self.business("business_a", enabled=("missing",))
        value = json.loads((self.businesses / "business_a.json").read_text())
        value["currency"] = "usd"
        self.write(self.businesses / "business_a.json", value)
        report = validate_business(BusinessRegistry(self.businesses, self.sources), "business_a")
        self.assertEqual({issue.code for issue in report.issues},
                         {"enabled_source_missing", "invalid_currency"})

    def test_validate_all_includes_orphan_source_business(self):
        self.source("orphan.json", "orphan_business")
        environment = {"PULSE_BUSINESSES_DIR": str(self.businesses),
                       "PULSE_SOURCES_DIR": str(self.sources)}
        output = StringIO()
        with patch.dict(os.environ, environment), redirect_stdout(output):
            self.assertEqual(main(("validate-all",)), 1)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload[0]["business_id"], "orphan_business")
        self.assertEqual(payload[0]["issues"][0]["code"], "business_not_found")


if __name__ == "__main__":
    unittest.main()
