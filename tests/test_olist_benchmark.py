"""Phase 6.4A Olist benchmark contracts (offline fixture mode)."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.benchmarks.olist import (MANIFEST_PATH, OLIST_BUSINESS_ID,
                                  OlistManifest, OlistSourceAdapter,
                                  SAFE_STATUS_MAP, run_benchmark)
from src.onboarding.models import IngestionEnvelope
from src.onboarding.adapters import adapter_for
from src.onboarding.registry import BusinessRegistry, validate_business
from src.warehouse.load_olist_benchmark import benchmark_rows


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class OlistManifestTests(unittest.TestCase):
    def test_manifest_declares_required_and_deferred_roles(self):
        manifest = OlistManifest.load()
        self.assertEqual(set(manifest.required_files), {
            "orders", "order_items", "payments", "products", "sellers",
            "customers", "category_translation",
        })
        self.assertEqual(set(manifest.raw["deferred_files"]), {"reviews", "geolocation"})
        self.assertFalse(manifest.validate(manifest.root(fixture=True)))

    def test_missing_dataset_has_clear_failure(self):
        with TemporaryDirectory() as directory:
            missing = Path(directory) / "not-downloaded"
            errors = OlistManifest.load().validate(missing)
        self.assertIn("dataset directory is missing", errors[0])

    def test_public_data_is_ignored_and_fixture_is_eligible(self):
        ignore = (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8")
        self.assertIn("data/*", ignore)
        self.assertIn("!data/fixtures/olist/", ignore)


class OlistAdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest = OlistManifest.load()
        cls.adapter = OlistSourceAdapter(cls.manifest.root(fixture=True), cls.manifest)

    def test_adapter_uses_existing_source_boundary_and_is_offline(self):
        self.assertFalse(self.adapter.validate_config())
        self.assertTrue(self.adapter.healthcheck().healthy)
        self.assertIn("local files only", self.adapter.healthcheck().message)
        self.assertEqual(self.adapter.config.business_id, OLIST_BUSINESS_ID)
        self.assertEqual(self.adapter.config.source_type, "commerce_dataset")

    def test_benchmark_business_and_source_are_registered(self):
        registry = BusinessRegistry()
        report = validate_business(registry, OLIST_BUSINESS_ID)
        self.assertTrue(report.valid)
        self.assertEqual(report.source_count, 1)
        configured = registry.sources_for(OLIST_BUSINESS_ID)[0]
        self.assertIsInstance(adapter_for(configured), OlistSourceAdapter)

    def test_envelope_has_deterministic_cross_business_safe_identity(self):
        row = next(iter(self.adapter.rows("orders")))
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        first = self.adapter.envelope("orders", row, now)
        second = self.adapter.envelope("orders", row, now)
        self.assertIsInstance(first, IngestionEnvelope)
        self.assertEqual(first.record_id, second.record_id)
        self.assertEqual(first.payload["source_file"], "olist_orders_dataset.csv")
        self.assertIn("native_record", first.payload)

    def test_safe_status_mapping_does_not_force_unavailable(self):
        self.assertIsNone(SAFE_STATUS_MAP["unavailable"])
        self.assertEqual(SAFE_STATUS_MAP["delivered"], "DELIVERED")
        self.assertEqual(SAFE_STATUS_MAP["canceled"], "CANCELLED")


class OlistFixtureBenchmarkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.report = run_benchmark(fixture=True, write_outputs=False)
        cls.quality = {row["code"]: row for row in cls.report["quality"]}

    def test_fixture_is_fast_offline_and_business_isolated(self):
        self.assertFalse(self.report["network_access"])
        self.assertEqual(self.report["business_id"], OLIST_BUSINESS_ID)
        self.assertEqual(self.report["input_rows"]["orders"], 5)
        self.assertEqual(self.report["rejected_rows"], 0)

    def test_multi_line_seller_and_payment_fanout(self):
        metrics = self.report["metrics"]
        self.assertEqual(metrics["orders_with_multiple_items"], 1)
        self.assertEqual(metrics["orders_with_multiple_sellers"], 1)
        self.assertEqual(metrics["orders_with_multiple_payments"], 1)
        self.assertEqual(self.report["silver_rows"]["commerce_orders"], 5)
        self.assertEqual(metrics["merchandise_value_total"], 170.0)
        self.assertEqual(metrics["customer_freight_charge_total"], 27.0)
        self.assertEqual(metrics["commerce_calculated_total"], 197.0)
        self.assertEqual(metrics["payment_total"], 202.5)

    def test_orders_without_lines_remain_valid(self):
        self.assertEqual(self.quality["order_missing_lines"]["count"], 2)
        self.assertEqual(self.report["economics"]["order_rows"], 5)

    def test_payment_is_separate_and_mismatch_is_diagnostic(self):
        self.assertEqual(self.report["silver_rows"]["payments"], 4)
        self.assertEqual(self.report["metrics"]["payment_mismatch_over_1_00"], 1)
        self.assertEqual(self.quality["payment_reconciliation_over_1_00"]["classification"],
                         "RECONCILIATION")

    def test_temporal_business_outcome_and_defects_are_distinct(self):
        self.assertEqual(self.quality["late_delivery"]["classification"], "BUSINESS_OUTCOME")
        self.assertEqual(self.quality["delivery_before_carrier"]["classification"], "DATA_DEFECT")
        self.assertEqual(self.quality["carrier_before_approval"]["severity"], "WARNING")

    def test_referential_integrity_and_missing_category(self):
        for name in ("items_order", "payments_order", "orders_customer", "items_product", "items_seller"):
            self.assertEqual(self.quality[f"referential_integrity_{name}"]["count"], 0)
        self.assertEqual(self.quality["product_missing_category"]["count"], 1)

    def test_no_cod_remittance_profit_or_attribution_fabrication(self):
        economics = self.report["economics"]
        self.assertEqual(economics["statuses"], {"INCOMPLETE_COSTS": 5})
        self.assertFalse(economics["cod_fabricated"])
        self.assertFalse(economics["remittance_fabricated"])
        self.assertFalse(economics["profit_calculated"])
        self.assertFalse(economics["attribution_available"])

    def test_fixture_exercises_existing_operations_and_economics_builders(self):
        self.assertTrue(self.report["operations"]["core_compatibility_exercised"])
        self.assertEqual(self.report["operations"]["order_operations_current"], 5)
        self.assertEqual(self.report["anomaly"]["status"], "INSUFFICIENT_HISTORY")
        self.assertTrue(self.report["anomaly"]["thresholds_unchanged"])

    def test_written_layers_are_deterministic_and_privacy_minimized(self):
        with TemporaryDirectory() as directory:
            report = run_benchmark(fixture=True, output_root=Path(directory), write_outputs=True)
            order = json.loads((Path(directory) / "silver" / "commerce_orders.jsonl")
                               .read_text(encoding="utf-8").splitlines()[0])
            customer = json.loads((Path(directory) / "silver" / "customers.jsonl")
                                  .read_text(encoding="utf-8").splitlines()[0])
            payment = json.loads((Path(directory) / "silver" / "payments.jsonl")
                                 .read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(order["commerce_calculated_total"], 120.0)
            self.assertEqual(order["payment_total"], 120.0)
            self.assertEqual(payment["currency"], "BRL")
            self.assertNotIn("zip", json.dumps(customer).lower())
            self.assertNotIn("city", customer)
            self.assertTrue((Path(directory) / "bronze" / "orders.jsonl").is_file())
            self.assertEqual(report["bronze_rows"], 25)
            warehouse = benchmark_rows(Path(directory))
            self.assertEqual(len(warehouse["olist_orders_by_status"]), 4)
            self.assertEqual(warehouse["olist_economic_completeness"][0]["business_id"],
                             OLIST_BUSINESS_ID)


@unittest.skipUnless(os.getenv("RUN_OLIST_FULL_BENCHMARK") == "1",
                     "set RUN_OLIST_FULL_BENCHMARK=1 for the local public dataset")
class OlistFullBenchmarkAcceptanceTests(unittest.TestCase):
    def test_full_local_acceptance_reconciles_manifest(self):
        report = run_benchmark(write_outputs=False, enforce_acceptance=True)
        self.assertTrue(all(item["passed"] for item in report["acceptance"].values()))
        self.assertFalse(report["operations"]["core_compatibility_exercised"])


if __name__ == "__main__":
    unittest.main()
