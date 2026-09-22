"""Phase 6.5A synthetic and opt-in real-data acceptance tests."""

from __future__ import annotations

from collections import Counter
import json
import os
from pathlib import Path
import unittest

from src.pilots.sama import (PRIVATE_ROOT, build_pilot, load_private_pilot,
                             phone_linkage_key, profile_tiktok_exports,
                             resolve_identity)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = PROJECT_ROOT / "data/fixtures/sama_pilot/sama_pilot_synthetic.json"


class SamaPilotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
        cls.fixture = fixture
        cls.result = build_pilot(fixture["lightfunnels"], fixture["leads"], fixture["orders"],
                                 hmac_key="synthetic-test-key")

    def test_intent_grain_deduplicates_order_item_fanout(self):
        facts = {row["lightfunnel_order_id"]: row for row in self.result.order_facts}
        self.assertEqual(len(facts), 10)
        self.assertEqual(facts["LF-001"]["initial_quantity"], 2)
        self.assertEqual(facts["LF-002"]["initial_quantity"], 4)
        self.assertEqual(facts["LF-001"]["source_item_price"], 199)
        self.assertEqual(facts["LF-001"]["compare_at_value"], 398)
        self.assertEqual(facts["LF-001"]["utm_source"], "synthetic-social")
        checks = {row["check_name"]: row["issue_count"] for row in self.result.data_quality}
        self.assertEqual(checks["duplicate_lightfunnels_raw_rows"], 1)
        self.assertEqual(checks["duplicate_lightfunnels_order_item_fanout"], 1)

    def test_identity_resolution_is_conservative_and_pseudonymous(self):
        statuses = {row["lightfunnel_order_id"]: row["match_status"]
                    for row in self.result.identity_resolution}
        self.assertEqual(statuses["LF-008"], "AMBIGUOUS")
        self.assertEqual(statuses["LF-009"], "UNMATCHED")
        self.assertEqual(statuses["LF-001"], "MATCHED_HIGH_CONFIDENCE")
        linkage = next(row["linkage_key"] for row in self.result.identity_resolution
                       if row["lightfunnel_order_id"] == "LF-001")
        self.assertEqual(len(linkage), 64)
        self.assertEqual(linkage, phone_linkage_key("+999000001", "synthetic-test-key"))

    def test_one_cod_lead_cannot_be_assigned_to_two_intents(self):
        shared = "a" * 64
        orders = [
            {"order_id": order_id, "created_at": None, "initial_quantity": 2,
             "initial_total": 199.0, "country": "SA", "linkage_key": shared}
            for order_id in ("LF-A", "LF-B")
        ]
        leads = [{"lead_id": "LEAD-ONLY", "created_at": None, "quantity": 2,
                  "total": 199.0, "country": "SA", "linkage_key": shared}]
        resolved = resolve_identity(orders, leads)
        self.assertEqual({row["match_status"] for row in resolved}, {"AMBIGUOUS"})
        self.assertTrue(all(row["cod_lead_id"] is None for row in resolved))

    def test_source_truth_and_business_outcomes_remain_distinct(self):
        facts = {row["lightfunnel_order_id"]: row for row in self.result.order_facts}
        self.assertEqual(facts["LF-003"]["lead_status"], "Wrong")
        self.assertEqual(facts["LF-004"]["lead_status"], "Expired")
        self.assertEqual(facts["LF-005"]["lead_status"], "Cancelled price")
        self.assertFalse(facts["LF-003"]["confirmed"])
        self.assertTrue(facts["LF-007"]["quantity_changed"])
        self.assertEqual((facts["LF-007"]["initial_quantity"], facts["LF-007"]["final_quantity"]), (2, 1))

    def test_cost_rules_keep_native_and_usd_money_separate(self):
        facts = {row["lightfunnel_order_id"]: row for row in self.result.order_facts}
        delivered = facts["LF-001"]
        self.assertEqual(delivered["cash_collected"], 199)
        self.assertAlmostEqual(delivered["cod_fee_native"], 9.95)
        self.assertAlmostEqual(delivered["product_cogs_usd"], 5.60)
        self.assertEqual(delivered["call_center_cost_usd"], 3)
        self.assertEqual(delivered["logistics_cost_usd"], 4.99)
        self.assertEqual(delivered["economic_status"], "FX_REQUIRED")
        returned = facts["LF-002"]
        self.assertEqual(returned["product_cogs_usd"], 0)
        self.assertEqual(returned["logistics_cost_usd"], 2.99)
        stockout = facts["LF-006"]
        self.assertEqual(stockout["logistics_cost_usd"], 0)
        self.assertEqual(stockout["product_cogs_usd"], 0)

    def test_lifecycle_after_cohort_end_is_retained(self):
        fact = next(row for row in self.result.order_facts if row["lightfunnel_order_id"] == "LF-010")
        self.assertTrue(fact["delivered"])
        event = next(row for row in self.result.operational_events
                     if row["order_id"] == "LF-010" and row["canonical_status"] == "DELIVERED")
        self.assertEqual(event["event_at"].month, 8)

    def test_rule_events_are_idempotent_and_total_usd_is_not_authoritative(self):
        second = build_pilot(self.fixture["lightfunnels"], self.fixture["leads"], self.fixture["orders"],
                             hmac_key="synthetic-test-key")
        self.assertEqual([row["cost_component_id"] for row in self.result.cost_components],
                         [row["cost_component_id"] for row in second.cost_components])
        serialized = json.dumps(self.result.order_facts, default=str)
        self.assertNotIn("total_usd", serialized.lower())
        self.assertNotIn("53.00", serialized)

    def test_no_raw_pii_fields_escape_the_adapter(self):
        forbidden = {"phone", "email", "name", "address", "tracking_number"}
        for collection in self.result.__dict__.values() if hasattr(self.result, "__dict__") else (
                self.result.order_facts, self.result.identity_resolution, self.result.data_quality,
                self.result.commerce_orders, self.result.order_lines, self.result.operational_events,
                self.result.shipments, self.result.cash_collections, self.result.product_costs,
                self.result.cost_components):
            if isinstance(collection, tuple):
                for row in collection:
                    self.assertFalse(forbidden & set(row), forbidden & set(row))
                    if "tracking_reference" in row:
                        self.assertIsNone(row["tracking_reference"])

    def test_manual_airflow_dag_has_safe_ordered_tasks(self):
        source = (PROJECT_ROOT / "airflow/dags/pulse_sama_cod_pilot.py").read_text(encoding="utf-8")
        self.assertIn('dag_id="pulse_sama_real_cod_pilot"', source)
        self.assertIn("schedule=None", source)
        for task_id in ("validate_private_sources", "load_sanitized_pilot", "validate_tiktok_source",
                        "load_tiktok_marketing", "run_dbt", "test_dbt"):
            self.assertIn(f'task_id="{task_id}"', source)
        for dependency in (
            "validate_private_sources\n        >> load_sanitized_pilot",
            "load_sanitized_pilot\n        >> validate_tiktok_source",
            "validate_tiktok_source\n        >> load_tiktok_marketing",
            "load_tiktok_marketing\n        >> run_dbt",
            "run_dbt\n        >> test_dbt",
        ):
            self.assertIn(dependency, source)


@unittest.skipUnless(os.environ.get("RUN_SAMA_PILOT_FULL_ACCEPTANCE") == "1",
                     "Set RUN_SAMA_PILOT_FULL_ACCEPTANCE=1 to validate ignored local source files")
class SamaPilotRealDataAcceptanceTests(unittest.TestCase):
    def test_private_source_structure_without_printing_records(self):
        result = load_private_pilot(PRIVATE_ROOT)
        profile = result.source_profile
        self.assertEqual(profile["raw_rows"], 5684)
        self.assertEqual(profile["unique_order_ids"], 1073)
        self.assertEqual(profile["distinct_item_ids"], 2378)
        self.assertEqual(profile["cod_lead_rows"], 2999)
        self.assertEqual(profile["unique_cod_lead_ids"], 2999)
        self.assertEqual(profile["cod_order_rows"], 2080)
        self.assertEqual(profile["unique_cod_order_references"], 2080)
        self.assertEqual(profile["cod_order_lead_ids_present"], 2079)
        self.assertTrue({"Confirmed", "Wrong", "Cancelled", "Expired", "Cancelled price", "Black listed"}
                        <= set(profile["cod_lead_statuses"]))
        self.assertTrue({"Delivered", "Return", "Out of stock", "Pending", "Cancel"}
                        <= set(profile["cod_order_statuses"]))
        self.assertEqual(len(result.identity_resolution), 1073)
        self.assertEqual(sum(Counter(row["match_status"] for row in result.identity_resolution).values()), 1073)

    def test_tiktok_is_profiled_but_not_daily_ingested(self):
        profiles = profile_tiktok_exports(PRIVATE_ROOT)
        self.assertEqual({row["observed_grain"] for row in profiles.values()},
                         {"campaign_period_aggregate", "ad_period_aggregate"})
        self.assertFalse(any(row["daily_compatible"] for row in profiles.values()))


if __name__ == "__main__":
    unittest.main()
