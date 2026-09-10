"""Focused Phase 6.2 commerce operations and COD tests."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.onboarding.adapters import adapter_for
from src.onboarding.contracts import contract_for
from src.onboarding.registry import BusinessRegistry
from src.operations.adapters import (CommerceOperationsSourceAdapter, STATUS_MAPPINGS,
                                     lookback_start)
from src.operations.models import (OperationalEvent, OperationalStatus, event_latest_revisions,
                                   latest_order_events, operational_kpis, valid_transition)
from src.operations.pipeline import (GOLD_SCHEMAS, OPERATIONS_GOLD_TABLES, SILVER_SCHEMAS,
    OperationsPaths, build_gold_rows, extract_registered_operations, normalize_operations, run_pipeline)
from src.quality.models import QualityContext, Status
from src.quality.anomaly import AnomalyStatus, MetricSeries, evaluate
from src.quality.anomaly_runner import policy_for
from src.quality.operations import check_operations
from src.quality.runner import run_quality_checks
from src.quality.datasets import operations_gold_rules, operations_silver_rules
from src.warehouse.load_operations import OPERATIONS_GRAINS, OPERATIONS_TABLE_SPECS


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class OperationsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = BusinessRegistry(ROOT / "config" / "businesses", ROOT / "config" / "sources")
        cls.extractions = extract_registered_operations(cls.registry)
        cls.silver = normalize_operations(cls.extractions, cls.registry)
        cls.gold = build_gold_rows(cls.silver)

    def test_contracts_are_versioned_and_role_specific(self):
        versions = {"commerce_orders":"commerce_orders_v1", "confirmation_events":"confirmation_events_v1",
                    "fulfillment_events":"fulfillment_events_v1", "delivery_events":"delivery_events_v1",
                    "cod_collections":"cod_collections_v1", "remittances":"remittances_v1"}
        for source_type, version in versions.items():
            contract = contract_for(source_type, version)
            self.assertTrue(contract.grain_name)
            self.assertEqual(contract.unique_grain, ("external_event_id",))

    def test_every_role_reuses_source_adapter_and_preserves_native_bronze(self):
        for adapter, envelopes in self.extractions:
            self.assertIsInstance(adapter, CommerceOperationsSourceAdapter)
            self.assertTrue(all(callable(getattr(adapter, name)) for name in
                                ("validate_config", "extract", "normalize", "healthcheck")))
            self.assertTrue(adapter.healthcheck().healthy)
            self.assertIn("provider_status", envelopes[0].payload)
            self.assertIn("received_at_utc", envelopes[0].payload)
            self.assertTrue(envelopes[0].record_id)

    def test_status_mapping_preserves_native_status(self):
        events = self.silver["operational_events"]
        row = next(row for row in events if row["provider_status"] == "customer_not_answering")
        self.assertEqual(row["canonical_status"], "UNREACHABLE")
        self.assertEqual(STATUS_MAPPINGS[row["source_type"]][row["provider_status"]],
                         OperationalStatus.UNREACHABLE)

    def test_valid_invalid_and_correction_transitions(self):
        self.assertTrue(valid_transition("CONFIRMED", "SHIPPED"))
        self.assertTrue(valid_transition("REFUSED", "DELIVERED"))
        self.assertTrue(valid_transition("DELIVERED", "RETURNED_TO_ORIGIN"))
        self.assertFalse(valid_transition("CANCELLED", "SHIPPED"))
        self.assertFalse(valid_transition("DELIVERED", "IN_TRANSIT"))
        self.assertTrue(valid_transition("DELIVERED", "IN_TRANSIT", correction=True))

    def test_duplicate_event_is_idempotent_and_revisions_are_preserved(self):
        raw = [e for _, batch in self.extractions for e in batch
               if e.payload["external_event_id"] == "del_104_try3"
               and e.business_id == "operations_demo_a"]
        self.assertEqual(len(raw), 2)
        self.assertEqual(len({e.record_id for e in raw}), 1)
        canonical = [e for e in self.silver["operational_events"]
                     if e["external_event_id"] == "del_104_try3" and e["business_id"] == "operations_demo_a"]
        self.assertEqual(len(canonical), 1)
        corrections = [e for e in self.silver["operational_events"]
                       if e["external_event_id"] == "del_104_route_state"]
        self.assertEqual({e["revision"] for e in corrections}, {1, 2})

    def test_occurrence_time_beats_out_of_order_receive_time(self):
        current = next(row for row in self.gold["order_operations_current"]
                       if row["business_id"] == "operations_demo_a" and row["order_id"] == "ord_100")
        self.assertEqual(current["delivery_status"], "DELIVERED")
        transit = next(e for e in self.silver["operational_events"]
                       if e["business_id"] == "operations_demo_a" and e["external_event_id"] == "del_100_transit")
        delivered = next(e for e in self.silver["operational_events"]
                         if e["business_id"] == "operations_demo_a" and e["external_event_id"] == "del_100_final")
        self.assertGreater(transit["received_at_utc"], delivered["received_at_utc"])
        self.assertLess(transit["event_at"], delivered["event_at"])

    def test_late_correction_changes_projection_without_erasing_history(self):
        current = next(row for row in self.gold["order_operations_current"]
                       if row["business_id"] == "operations_demo_a" and row["order_id"] == "ord_105")
        self.assertEqual(current["delivery_status"], "RETURNED_TO_ORIGIN")
        self.assertEqual(len([e for e in self.silver["operational_events"]
                             if e["business_id"] == "operations_demo_a"
                             and e["external_event_id"] in {"del_105_final", "del_105_return"}]), 2)

    def test_multiple_attempts_and_split_shipments_remain_separate(self):
        current = next(row for row in self.gold["order_operations_current"]
                       if row["business_id"] == "operations_demo_a" and row["order_id"] == "ord_104")
        self.assertEqual((current["delivery_attempts"], current["shipment_count"]), (3, 2))
        attempts = [e["attempt_number"] for e in self.silver["operational_events"]
                    if e["business_id"] == "operations_demo_a" and e["order_id"] == "ord_104"
                    and e.get("attempt_number")]
        self.assertEqual(attempts, [1, 2, 3])
        green = next(row for row in self.gold["delivery_performance"]
                     if row["business_id"] == "operations_demo_a" and row["cohort_date"].isoformat() == "2026-01-02"
                     and row["courier"] == "courier_green")
        self.assertEqual((green["delivered_orders"], green["delivery_attempts"]), (0, 0))

    def test_confirmation_outcomes_are_distinct_from_delivery_outcomes(self):
        events = self.silver["operational_events"]
        rejected = next(e for e in events if e["business_id"] == "operations_demo_a"
                        and e["order_id"] == "ord_101" and e["source_type"] == "confirmation_events")
        refused = next(e for e in events if e["business_id"] == "operations_demo_a"
                       and e["canonical_status"] == "REFUSED")
        self.assertEqual(rejected["canonical_status"], "REJECTED_CONFIRMATION")
        self.assertEqual(refused["source_type"], "delivery_events")

    def test_cod_and_remittance_are_distinct_and_many_orders_link_to_one_remittance(self):
        remittance = next(r for r in self.silver["remittances"]
                          if r["business_id"] == "operations_demo_a" and r["remittance_id"] == "rem_100")
        self.assertEqual((remittance["gross_collected"], remittance["net_remitted"]), (240.0, 211.0))
        self.assertEqual(len(json.loads(remittance["order_links_json"])), 2)
        pending = sum(r["remittance_pending_amount"] for r in self.gold["remittance_performance"])
        self.assertEqual(pending, 450.0)

    def test_business_currency_and_provider_identity_isolation(self):
        shared_orders = [r for r in self.silver["commerce_orders"] if r["order_id"] == "ord_100"]
        self.assertEqual({r["business_id"] for r in shared_orders}, {"operations_demo_a", "operations_demo_b"})
        self.assertEqual({r["currency"] for r in shared_orders}, {"SAR", "AED"})
        shared_shipments = [r for r in self.silver["shipments"] if r["shipment_id"] == "shp_100"]
        self.assertEqual(len(shared_shipments), 2)
        event_ids = self.silver["operational_events"]
        self.assertEqual(len(event_ids), len({r["event_id"] for r in event_ids}))
        one = OperationalEvent(business_id="operations_demo_a", source_type="delivery_events", source_id="a",
            provider="provider_a", event_id="1", external_event_id="123", order_id="123", event_type="delivery",
            canonical_status=OperationalStatus.DELIVERED, provider_status="done", event_at=NOW, received_at_utc=NOW)
        two = OperationalEvent(business_id="operations_demo_a", source_type="delivery_events", source_id="b",
            provider="provider_b", event_id="2", external_event_id="123", order_id="123", event_type="delivery",
            canonical_status=OperationalStatus.DELIVERED, provider_status="done", event_at=NOW, received_at_utc=NOW)
        self.assertEqual(len(event_latest_revisions((one, two))), 2)

    def test_kpi_denominators_and_zero_denominators(self):
        values = operational_kpis(eligible_orders=10, confirmed_orders=7, shipped_orders=5,
            delivered_orders=4, refused_orders=1, returned_orders=1, unreachable_orders=2,
            delivery_attempts=8, cash_expected=400, cash_collected=360)
        self.assertEqual(values, {"confirmation_rate":.7,"ship_rate":5/7,"delivery_rate":.8,
            "refusal_rate":.2,"return_rate":.25,"unreachable_rate":.2,
            "average_delivery_attempts":1.6,"cash_collection_rate":.9})
        zero = operational_kpis(eligible_orders=0, confirmed_orders=0, shipped_orders=0,
            delivered_orders=0, refused_orders=0, returned_orders=0, unreachable_orders=0,
            delivery_attempts=0, cash_expected=0, cash_collected=0)
        self.assertTrue(all(value is None for value in zero.values()))

    def test_time_to_metrics_are_occurrence_based(self):
        confirmation = [r for r in self.gold["confirmation_performance"]
                        if r["business_id"] == "operations_demo_a" and r["confirmed_orders"]]
        delivery = [r for r in self.gold["delivery_performance"]
                    if r["business_id"] == "operations_demo_a" and r["delivered_orders"]]
        self.assertTrue(all(r["average_time_to_confirm_hours"] >= 0 for r in confirmation))
        self.assertTrue(all(r["average_time_to_ship_hours"] >= 0 and
                            r["average_time_to_deliver_hours"] >= 0 for r in delivery))

    def test_quality_accepts_business_outcomes_and_flags_data_defects(self):
        self.assertEqual(check_operations(self.silver), ())
        corrupt = {name: [dict(row) for row in rows] for name, rows in self.silver.items()}
        corrupt["cash_collections"][0]["cash_collected"] = -1
        corrupt["operational_events"].append(dict(corrupt["operational_events"][0]))
        codes = {issue.code for issue in check_operations(corrupt)}
        self.assertIn("negative_collection_amount", codes)
        self.assertIn("duplicate_event", codes)
        self.assertNotIn("customer_refused", codes)

    def test_warehouse_grains_and_schemas_are_business_aware(self):
        self.assertEqual({spec.name for spec in OPERATIONS_TABLE_SPECS}, set(GOLD_SCHEMAS))
        for spec in OPERATIONS_TABLE_SPECS:
            self.assertIn("business_id", OPERATIONS_GRAINS[spec.name])
            rows = self.gold[spec.name]
            self.assertEqual(len(rows), len({tuple(row[col] for col in OPERATIONS_GRAINS[spec.name]) for row in rows}))

    def test_registry_discovery_cli_airflow_dbt_and_metabase_are_generic(self):
        from src.orchestration.business_sources import discover_enabled_sources
        discovered = discover_enabled_sources(self.registry)
        self.assertEqual(sum(item.source_type in STATUS_MAPPINGS for item in discovered), 12)
        dag = (ROOT / "airflow/dags/pulse_analytics_pipeline.py").read_text().lower()
        self.assertIn("src.operations.pipeline build", dag)
        self.assertNotIn("operations_demo_a", dag)
        self.assertIn("pulse commerce operations", (ROOT / "bi/setup_metabase.py").read_text().lower())
        self.assertEqual(len(list((ROOT / "bi/operations_queries").glob("*.sql"))), 14)

    def test_incremental_lookback_is_inclusive_and_safe(self):
        self.assertEqual(lookback_start(datetime(2026,1,10,tzinfo=timezone.utc), 7),
                         datetime(2026,1,3,tzinfo=timezone.utc))
        with self.assertRaises(ValueError): lookback_start(NOW, -1)

    def test_operational_rate_short_history_is_insufficient(self):
        result = evaluate(MetricSeries(metric_name="delivery_rate", dataset_name="delivery_performance",
            layer="analytics", current_value=.8, history=(.75,), observed_at_utc=NOW,
            history_observed_at_utc=(datetime(2025,12,31,tzinfo=timezone.utc),),
            dimensions={"business_id":"operations_demo_a", "courier":"courier_blue"},
            current_sample_size=10, history_sample_sizes=(10,)), policy_for("delivery_rate", 3),
            __import__("uuid").uuid5(__import__("uuid").NAMESPACE_URL, "ops-anomaly"))
        self.assertEqual(result.status, AnomalyStatus.INSUFFICIENT_HISTORY)


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS", "1") == "1", "Spark tests disabled")
class OperationsSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src.analytics.gold_build import build_gold_spark_session
        cls.spark = build_gold_spark_session(app_name="pulse-operations-tests", master="local[1]")
        cls.spark.conf.set("spark.sql.shuffle.partitions", "1")

    @classmethod
    def tearDownClass(cls): cls.spark.stop()

    def test_offline_bronze_silver_gold_and_quality_flow(self):
        registry = BusinessRegistry(ROOT / "config/businesses", ROOT / "config/sources")
        with TemporaryDirectory() as directory:
            root = Path(directory)
            paths = OperationsPaths(*(root / name for name in (
                "bronze", *SILVER_SCHEMAS, *OPERATIONS_GOLD_TABLES)))
            counts = run_pipeline(registry, paths=paths, spark=self.spark)
            self.assertEqual(counts["commerce_orders"], 11)
            self.assertEqual(counts["operational_events"], 51)
            bronze = self.spark.read.parquet(str(paths.bronze))
            self.assertEqual(bronze.count(), 51)
            self.assertIn("provider_status", json.loads(bronze.first().payload))
            for name, schema in SILVER_SCHEMAS.items():
                frame = self.spark.read.schema(schema).parquet(str(getattr(paths, name)))
                results = run_quality_checks(frame, operations_silver_rules(name),
                    QualityContext(dataset_name=name, layer="silver"))
                self.assertTrue(all(result.status == Status.PASS for result in results), name)
            for name, schema in GOLD_SCHEMAS.items():
                frame = self.spark.read.schema(schema).parquet(str(getattr(paths, name)))
                results = run_quality_checks(frame, operations_gold_rules(name),
                    QualityContext(dataset_name=name, layer="gold"))
                self.assertTrue(all(result.status == Status.PASS for result in results), name)


if __name__ == "__main__": unittest.main()
