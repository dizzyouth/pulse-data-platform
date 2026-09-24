"""Phase 6.6A deterministic decision-intelligence contracts."""

from __future__ import annotations

from datetime import date
from pathlib import Path
import unittest

from src.intelligence.cli import render_markdown
from src.intelligence.models import DecisionSignal
from src.quality.anomaly_runner import policy_for


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MART_ROOT = PROJECT_ROOT / "dbt" / "models" / "marts"
QUERY_ROOT = PROJECT_ROOT / "bi" / "sama_intelligence_queries"


def signal(**overrides) -> DecisionSignal:
    values = {
        "signal_id": "business|RETURN_PRESSURE",
        "business_id": "sama_cod_pilot",
        "as_of_date": date(2026, 7, 31),
        "scope_type": "BUSINESS",
        "scope_id": "sama_cod_pilot",
        "scope_name": "sama_cod_pilot",
        "signal_type": "RETURN_PRESSURE",
        "signal_category": "FULFILLMENT",
        "priority": "HIGH",
        "confidence": "HIGH",
        "metric_name": "returned_orders",
        "observed_value": 12.0,
        "baseline_value": 10.0,
        "absolute_gap": 2.0,
        "relative_gap": 0.2,
        "sample_size": 40,
        "impact_order_count": 12.0,
        "evidence_summary": "Observed evidence.",
        "why_it_matters": "Observed gap needs attention.",
        "recommended_next_step": "Investigate the downstream evidence.",
        "limitation": "Current data cannot establish cause.",
        "causal_claim": False,
    }
    values.update(overrides)
    return DecisionSignal(**values)


class SamaIntelligenceContractTests(unittest.TestCase):
    def test_signal_contract_rejects_causality_and_low_confidence_high_priority(self) -> None:
        with self.assertRaisesRegex(ValueError, "causal"):
            signal(causal_claim=True)
        with self.assertRaisesRegex(ValueError, "LOW sample"):
            signal(confidence="LOW", priority="HIGH")

    def test_brief_is_rendered_from_signal_values_and_includes_limits(self) -> None:
        brief = render_markdown([signal()], "sama_cod_pilot")
        self.assertIn("RETURN_PRESSURE — HIGH", brief)
        self.assertIn("Observed vs baseline: 12 vs 10", brief)
        self.assertIn("12.00 observed affected orders", brief)
        self.assertIn("Current data cannot establish cause", brief)

    def test_business_leakage_stages_are_explicit_and_not_all_operational(self) -> None:
        sql = (MART_ROOT / "sama_pilot_business_leakage.sql").read_text(encoding="utf-8").lower()
        for stage in (
            "platform_vs_observed", "identity_unresolved", "matched_not_confirmed",
            "confirmed_not_shipped", "returned_after_shipment", "shipped_terminal_unresolved",
        ):
            self.assertIn(stage, sql)
        self.assertIn("true, false", sql)
        self.assertNotIn("lost revenue", sql)

    def test_campaign_diagnostics_use_leave_one_out_aggregate_baselines(self) -> None:
        sql = (MART_ROOT / "sama_pilot_campaign_diagnostics.sql").read_text(
            encoding="utf-8"
        ).lower()
        self.assertIn("total_confirmed_orders - campaigns.confirmed_orders", sql)
        self.assertIn("total_shipped_orders - campaigns.shipped_orders", sql)
        self.assertIn("total_spend_usd - campaigns.spend_usd", sql)
        self.assertIn("where is_target_campaign", sql)
        self.assertNotIn("ad_group_id", sql)
        self.assertNotIn("ad_id", sql)
        for band in ("confirmation_sample_band", "fulfillment_sample_band",
                     "acquisition_sample_band", "cost_delivered_sample_band"):
            self.assertIn(band, sql)

    def test_signals_are_deterministic_observational_and_fx_safe(self) -> None:
        sql = (MART_ROOT / "sama_pilot_intelligence_signals.sql").read_text(
            encoding="utf-8"
        ).lower()
        self.assertIn("false as causal_claim", sql)
        self.assertIn("confidence = 'low'", sql)
        self.assertIn("row_number() over", sql)
        self.assertIn("signal_category = 'measurement'", sql)
        self.assertNotIn("random(", sql)
        for forbidden in ("business_roas", "contribution_margin", "net_profit"):
            self.assertNotIn(forbidden, sql)

    def test_real_daily_series_reuses_existing_anomaly_engine_with_narrow_policy(self) -> None:
        source = (PROJECT_ROOT / "src" / "quality" / "anomaly_sources.py").read_text(
            encoding="utf-8"
        )
        for metric in (
            "daily_target_spend", "lightfunnels_order_volume", "confirmed_order_volume",
            "delivered_order_volume", "returned_order_volume",
        ):
            self.assertIn(metric, source)
        self.assertEqual(policy_for("daily_target_spend", 7).minimum_absolute_deviation, 25)
        self.assertEqual(policy_for("returned_order_volume", 7).minimum_absolute_deviation, 5)

    def test_dashboard_is_separate_readable_and_preserves_prior_dashboards(self) -> None:
        source = (PROJECT_ROOT / "bi" / "setup_metabase.py").read_text(encoding="utf-8")
        self.assertIn("Pulse — Intelligence", source)
        self.assertIn("What Pulse Cannot Explain Yet", source)
        self.assertIn("Economic Safety — FX_REQUIRED", source)
        self.assertIn("_ensure_sama_pilot_dashboard(session_id, database_id)", source)
        self.assertIn("_ensure_sama_tiktok_dashboard(session_id, database_id)", source)
        self.assertIn("_ensure_sama_unified_dashboard(session_id, database_id)", source)
        queries = list(QUERY_ROOT.glob("*.sql"))
        self.assertEqual(len(queries), 12)
        for path in queries:
            text = path.read_text(encoding="utf-8").lower()
            self.assertIn("business_id", text, path.name)
            for forbidden in ("phone", "email", "address", "tracking_number"):
                self.assertNotIn(forbidden, text, path.name)

    def test_existing_manual_dag_is_extended_with_one_nonblocking_anomaly_task(self) -> None:
        source = (PROJECT_ROOT / "airflow" / "dags" / "pulse_sama_cod_pilot.py").read_text(
            encoding="utf-8"
        )
        self.assertIn('dag_id="pulse_sama_real_cod_pilot"', source)
        self.assertIn("schedule=None", source)
        for model in (
            "sama_pilot_business_leakage", "sama_pilot_campaign_diagnostics",
            "sama_pilot_intelligence_signals",
        ):
            self.assertIn(model, source)
        self.assertEqual(source.count("evaluate_pilot_anomalies_nonblocking"), 1)
        self.assertNotIn("--block-on-critical", source)


if __name__ == "__main__":
    unittest.main()
