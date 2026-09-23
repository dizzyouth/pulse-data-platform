"""Phase 6.5C unified business presentation contracts."""

from __future__ import annotations

import json
from pathlib import Path
import unittest

from src.pilots.sama import _utm_attributes


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MART_ROOT = PROJECT_ROOT / "dbt" / "models" / "marts"
QUERY_ROOT = PROJECT_ROOT / "bi" / "sama_unified_queries"


class SamaUnifiedContractsTests(unittest.TestCase):
    def test_one_canonical_utm_parser_handles_real_lightfunnels_shape(self) -> None:
        parsed = _utm_attributes(
            "source=tiktokid=1000000000000001campaign=Target+Campaignmedium=cpc"
        )
        self.assertEqual(parsed["utm_source"], "tiktok")
        self.assertEqual(parsed["utm_id"], "1000000000000001")
        self.assertEqual(parsed["utm_campaign"], "Target Campaign")
        self.assertEqual(parsed["utm_medium"], "cpc")

    def test_unified_marts_protect_attribution_grain_and_currency(self) -> None:
        marts = {
            path.name: path.read_text(encoding="utf-8").lower()
            for path in MART_ROOT.glob("sama_pilot_unified_*.sql")
        }
        self.assertEqual(set(marts), {
            "sama_pilot_unified_overview.sql",
            "sama_pilot_unified_daily.sql",
            "sama_pilot_unified_native_economics.sql",
        })
        for name, sql in marts.items():
            self.assertNotIn("ad_group_id", sql, name)
            self.assertNotIn("ad_id", sql, name)
        self.assertIn("full outer join", marts["sama_pilot_unified_daily.sql"])
        self.assertIn("lower(trim(utm_source)) = 'tiktok'", marts["sama_pilot_unified_overview.sql"])
        native = marts["sama_pilot_unified_native_economics.sql"]
        self.assertIn(
            "group by business_id, coalesce(currency, 'unspecified'), economic_status",
            native,
        )
        for forbidden in ("profit", "margin", "business_roas", "mer"):
            self.assertNotIn(forbidden, native)

    def test_known_usd_cost_formula_and_daily_reconciliation_are_tested(self) -> None:
        schema = (MART_ROOT / "marts.yml").read_text(encoding="utf-8").lower()
        macros = (PROJECT_ROOT / "dbt" / "macros" / "generic_tests.sql").read_text(
            encoding="utf-8"
        ).lower()
        self.assertIn("total_known_usd_cost - (marketing_spend_usd + known_operational_cost_usd)", schema)
        self.assertIn("sama_unified_daily_reconciles", schema)
        self.assertIn("sama_unified_fact_cohort_reconciles", schema)
        self.assertIn("sama_unified_native_reconciles", schema)
        self.assertIn("ref('sama_pilot_unified_overview')", macros)

    def test_dashboard_is_separate_aggregate_only_and_decision_safe(self) -> None:
        source = (PROJECT_ROOT / "bi" / "setup_metabase.py").read_text(encoding="utf-8")
        self.assertIn("Pulse — Unified Business Overview", source)
        self.assertIn("Delivered and Returned are sibling terminal outcomes", source)
        self.assertIn("Known USD Cost Total", source)
        self.assertIn("Native Currency — not converted", source)
        self.assertIn("FX_REQUIRED — Economic Completeness", source)
        self.assertIn("_ensure_sama_pilot_dashboard(session_id, database_id)", source)
        self.assertIn("_ensure_sama_tiktok_dashboard(session_id, database_id)", source)

        queries = list(QUERY_ROOT.glob("*.sql"))
        self.assertEqual(len(queries), 17)
        combined = "\n".join(path.read_text(encoding="utf-8").lower() for path in queries)
        self.assertNotIn("analytics.", combined)
        self.assertNotIn("ad_group_id", combined)
        self.assertNotIn("ad_id", combined)
        for forbidden in ("phone", "email", "address", "tracking"):
            self.assertNotIn(forbidden, combined)
        for path in queries:
            self.assertIn("business_id", path.read_text(encoding="utf-8").lower(), path.name)

    def test_cost_total_is_not_an_additive_chart_category(self) -> None:
        stack = (QUERY_ROOT / "usd_cost_stack.sql").read_text(encoding="utf-8").lower()
        total = (QUERY_ROOT / "known_usd_cost_total.sql").read_text(encoding="utf-8").lower()
        self.assertNotIn("total_known_usd_cost", stack)
        self.assertIn("total_known_usd_cost", total)

    def test_only_executive_measurement_limitations_are_selected(self) -> None:
        query = (QUERY_ROOT / "measurement_limitations.sql").read_text(encoding="utf-8").lower()
        expected = {
            "platform_conversions_minus_lightfunnels_orders",
            "ambiguous_identity_matches",
            "unmatched_identity_records",
            "campaign_day_boundary_observations",
            "tiktok_campaigns_outside_target_cohort",
            "zero_spend_rows_with_attributed_activity",
        }
        for check in expected:
            self.assertIn(check, query)
        self.assertNotIn("fully_inactive_ad_day_rows", query)

    def test_airflow_extends_the_existing_manual_dag_only(self) -> None:
        source = (PROJECT_ROOT / "airflow" / "dags" / "pulse_sama_cod_pilot.py").read_text(
            encoding="utf-8"
        )
        self.assertIn('dag_id="pulse_sama_real_cod_pilot"', source)
        self.assertIn("schedule=None", source)
        for model in (
            "sama_pilot_unified_overview",
            "sama_pilot_unified_daily",
            "sama_pilot_unified_native_economics",
        ):
            self.assertIn(model, source)


if __name__ == "__main__":
    unittest.main()
