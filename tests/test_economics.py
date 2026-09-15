"""Focused contracts and reconciliation tests for unified commerce economics."""

from __future__ import annotations

from datetime import date, datetime, timezone
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.economics.models import (AttributionLink, CostBasis, CostComponent,
                                  CostScope, CostType, ProductCost, safe_divide)
from src.economics.pipeline import (GOLD_SCHEMAS, SILVER_SCHEMAS, demo_rows,
                                    effective_product_cost, resolve_cost_components,
                                    _spark_values)
from src.onboarding.contracts import contract_for
from src.quality.economics import check_economics
from src.quality.datasets import economics_gold_rules, economics_silver_rules
from src.quality.models import QualityContext, Status
from src.quality.runner import run_quality_checks


class EconomicsModelTests(unittest.TestCase):
    def test_versioned_source_contracts_are_provider_neutral(self) -> None:
        for source_type in ("product_costs", "variable_cost_events", "attribution_links"):
            contract = contract_for(source_type, f"{source_type}_v1")
            self.assertEqual(contract.source_type, source_type)

    def test_entities_reject_invalid_amounts_ranges_and_weights(self) -> None:
        common = dict(business_id="b", source_id="s", provider="synthetic")
        with self.assertRaisesRegex(ValueError, "valid_to"):
            ProductCost(**common, cost_record_id="c", external_record_id="x", product_id="p",
                        variant_id=None, sku=None, unit_cogs=1, currency="USD",
                        valid_from=date(2026, 2, 1), valid_to=date(2026, 1, 31))
        with self.assertRaisesRegex(ValueError, "nonnegative"):
            CostComponent(**common, cost_component_id="c", external_record_id="x",
                          cost_type=CostType.SHIPPING, amount=-1, currency="USD",
                          effective_at=datetime.now(timezone.utc), cost_basis=CostBasis.ACTUAL,
                          cost_scope=CostScope.ORDER, order_id="o")
        with self.assertRaisesRegex(ValueError, "between zero and one"):
            AttributionLink(**common, attribution_link_id="a", external_record_id="x",
                            order_id="o", marketing_platform="meta_ads",
                            marketing_source_id="m", marketing_date=date(2026, 1, 1),
                            attribution_method="synthetic_fixture", attribution_weight=1.1,
                            linked_at=datetime.now(timezone.utc))

    def test_effective_dated_cogs_selects_each_side_of_boundary(self) -> None:
        costs = (
            {"business_id":"b", "currency":"USD", "sku":"A", "variant_id":None,
             "product_id":None, "unit_cogs":10.0, "valid_from":date(2026, 1, 1),
             "valid_to":date(2026, 1, 15), "revision":1, "cost_record_id":"old"},
            {"business_id":"b", "currency":"USD", "sku":"A", "variant_id":None,
             "product_id":None, "unit_cogs":12.0, "valid_from":date(2026, 1, 16),
             "valid_to":None, "revision":1, "cost_record_id":"new"},
        )
        line = {"business_id":"b", "currency":"USD", "sku":"A",
                "variant_id":None, "product_id":None}
        self.assertEqual(effective_product_cost(costs, line, date(2026, 1, 15))["unit_cogs"], 10)
        self.assertEqual(effective_product_cost(costs, line, date(2026, 1, 16))["unit_cogs"], 12)

    def test_actual_order_cost_precedes_configured_and_estimated_overlap(self) -> None:
        def row(identity, basis, scope, amount, revision=1):
            return {"business_id":"b", "source_id":"costs", "provider":"synthetic",
                    "external_record_id":identity, "cost_component_id":f"{identity}-{revision}",
                    "cost_basis":basis, "cost_scope":scope, "precedence_key":"shipping:o",
                    "amount":amount, "revision":revision}
        selected = resolve_cost_components((row("estimate", "ESTIMATED", "SCHEDULE", 5),
                                            row("configured", "CONFIGURED", "SHIPMENT", 7),
                                            row("actual", "ACTUAL", "ORDER", 9),
                                            row("actual", "ACTUAL", "ORDER", 11, 2)))
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["amount"], 11)

    def test_zero_denominators_remain_null(self) -> None:
        self.assertIsNone(safe_divide(10, 0))
        self.assertEqual(safe_divide(10, 2), 5)


class EconomicsFixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.extractions, cls.silver, cls.gold = demo_rows()
        cls.orders = {(row["business_id"], row["order_id"]): row
                      for row in cls.gold["order_economics"]}

    def test_offline_demo_is_deterministic_and_idempotent(self) -> None:
        extractions, silver, gold = demo_rows()
        self.assertEqual(self.silver, silver)
        self.assertEqual(self.gold, gold)
        self.assertEqual(sum(len(batch) for _, batch in extractions), 33)
        self.assertEqual(len(gold["order_economics"]), 11)
        for name, rows in {**silver, **gold}.items():
            self.assertEqual(len(rows), len({tuple(row.get(field.name) for field in
                (SILVER_SCHEMAS.get(name) or GOLD_SCHEMAS[name]).fields) for row in rows}))

    def test_multiline_split_shipment_attempts_do_not_multiply_values(self) -> None:
        order = self.orders[("operations_demo_a", "ord_104")]
        self.assertEqual((order["shipment_count"], order["delivery_attempts"]), (2, 3))
        self.assertEqual(order["order_value"], 200)
        self.assertEqual(order["product_cogs"], 70)
        self.assertEqual(order["cash_collected"], 200)
        self.assertEqual(order["allocated_marketing_spend"], 25)

    def test_cod_refusal_and_return_costs_are_losses_not_quality_failures(self) -> None:
        refused = self.orders[("operations_demo_a", "ord_103")]
        returned = self.orders[("operations_demo_a", "ord_105")]
        self.assertEqual((refused["delivered_order_value"], refused["cash_collected"]), (0, 0))
        self.assertEqual(refused["contribution_before_marketing"], -72)
        self.assertEqual(returned["delivered_order_value"], 0)
        codes = {issue.code for issue in check_economics(self.silver, self.gold)}
        self.assertNotIn("negative_contribution", codes)
        self.assertNotIn("refused_order", codes)

    def test_missing_cogs_is_explicit_and_contribution_is_unavailable(self) -> None:
        order = self.orders[("operations_demo_b", "ord_108")]
        self.assertFalse(order["cogs_complete"])
        self.assertEqual(order["missing_cogs_lines"], 1)
        self.assertIsNone(order["contribution_before_marketing"])
        self.assertEqual(order["economic_status"], "INCOMPLETE_COSTS")
        self.assertIn("missing_cogs", {issue.code for issue in check_economics(self.silver, self.gold)})

    def test_explicit_attribution_preserves_unattributed_orders_and_unlinked_spend(self) -> None:
        attributed = self.orders[("operations_demo_a", "ord_100")]
        unattributed = self.orders[("operations_demo_a", "ord_107")]
        self.assertTrue(attributed["has_marketing_attribution"])
        self.assertEqual(attributed["allocated_marketing_spend"], 30)
        self.assertFalse(unattributed["has_marketing_attribution"])
        self.assertIsNone(unattributed["allocated_marketing_spend"])
        unlinked = next(row for row in self.gold["attributed_campaign_economics"]
                        if row["campaign_id"] == "meta_unlinked")
        self.assertEqual((unlinked["campaign_spend"], unlinked["attributed_orders"]), (20, 0))

    def test_currency_mismatch_never_produces_cross_currency_contribution(self) -> None:
        order = self.orders[("operations_demo_b", "ord_100")]
        self.assertEqual((order["currency"], order["marketing_currency"]), ("AED", "GBP"))
        self.assertFalse(order["currency_compatible"])
        self.assertIsNone(order["contribution_after_marketing"])
        self.assertEqual(order["economic_status"], "CURRENCY_MISMATCH")

    def test_remittance_allocates_once_and_unremitted_delivery_remains_visible(self) -> None:
        allocated = [self.orders[("operations_demo_a", identity)]["net_remitted"]
                     for identity in ("ord_100", "ord_107")]
        self.assertAlmostEqual(sum(allocated), 211.0)
        pending = self.orders[("operations_demo_a", "ord_104")]
        self.assertEqual((pending["delivered_order_value"], pending["net_remitted"]), (200, 0))
        cohort = next(row for row in self.gold["business_economics_cohort"]
                      if row["business_id"] == "operations_demo_a"
                      and row["cohort_date"] == date(2026, 1, 2))
        self.assertEqual((cohort["delivered_but_unremitted_count"],
                          cohort["delivered_but_unremitted_value"]), (1, 200))

    def test_business_and_source_aware_identities_prevent_collisions(self) -> None:
        repeated = [row for row in self.gold["order_economics"] if row["order_id"] == "ord_100"]
        self.assertEqual({row["business_id"] for row in repeated},
                         {"operations_demo_a", "operations_demo_b"})
        ids = [row["cost_component_id"] for row in self.silver["cost_components"]]
        self.assertEqual(len(ids), len(set(ids)))

    def test_gold_reconciles_order_grain_and_campaign_spend(self) -> None:
        order_rows = self.gold["order_economics"]
        self.assertEqual(len(order_rows), len({(row["business_id"], row["order_source_id"],
                                               row["order_id"], row["currency"])
                                              for row in order_rows}))
        for cohort in self.gold["business_economics_cohort"]:
            members = [row for row in order_rows if row["business_id"] == cohort["business_id"]
                       and row["order_created_date"] == cohort["cohort_date"]
                       and row["currency"] == cohort["currency"]]
            self.assertAlmostEqual(sum(row["delivered_order_value"] for row in members),
                                   cohort["delivered_order_value"])
            self.assertAlmostEqual(sum(row["product_cogs"] for row in members), cohort["product_cogs"])
        campaign_spend = sum(row["campaign_spend"] for row in
                             self.gold["attributed_campaign_economics"])
        self.assertEqual(campaign_spend, 102)


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS", "1") == "1", "Spark tests disabled")
class EconomicsSparkFlowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from src.economics.pipeline import build_economics_spark_session
        cls.spark = build_economics_spark_session(app_name="pulse-economics-tests", master="local[1]")
        cls.spark.conf.set("spark.sql.shuffle.partitions", "1")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_synthetic_silver_gold_write_read_and_quality_flow(self) -> None:
        _, silver, gold = demo_rows()
        with TemporaryDirectory() as directory:
            for name, rows in {**silver, **gold}.items():
                schema = SILVER_SCHEMAS.get(name) or GOLD_SCHEMAS[name]
                path = Path(directory) / name
                self.spark.createDataFrame(_spark_values(rows, schema), schema).write.parquet(str(path))
                frame = self.spark.read.schema(schema).parquet(str(path))
                rules = economics_silver_rules(name) if name in SILVER_SCHEMAS else economics_gold_rules(name)
                results = run_quality_checks(frame, rules,
                    QualityContext(dataset_name=name,
                                   layer="silver" if name in SILVER_SCHEMAS else "gold"))
                self.assertFalse(any(result.status == Status.FAIL for result in results), name)
                if name == "order_economics":
                    warning = next(result for result in results if result.check_name == "missing_cogs")
                    self.assertEqual(warning.status, Status.WARN)


if __name__ == "__main__":
    unittest.main()
