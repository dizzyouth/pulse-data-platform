"""Static contracts for the local Metabase BI layer."""

from __future__ import annotations

import unittest
from unittest.mock import patch
from pathlib import Path

import yaml
from bi import setup_metabase as setup


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXPECTED_MARTS = {
    "marts.revenue_by_day",
    "marts.top_customers",
    "marts.top_products",
    "marts.funnel_performance",
    "marts.marketing_overview",
    "marts.campaign_performance",
    "marts.ad_performance",
    "marts.operations_overview",
    "marts.confirmation_operations",
    "marts.delivery_operations",
    "marts.cod_performance",
    "marts.remittance_operations",
    "marts.commerce_economics",
    "marts.unit_economics",
    "marts.economics_daily",
    "marts.campaign_economics",
    "marts.cod_economics",
    "marts.olist_orders_by_status",
    "marts.olist_commerce_daily",
    "marts.olist_payment_methods",
    "marts.olist_data_quality",
    "marts.olist_economic_completeness",
    "marts.uci_retail_daily",
    "marts.uci_invoice_summary",
    "marts.uci_line_classification",
    "marts.uci_country_distribution",
    "marts.uci_data_quality",
    "marts.uci_economic_completeness",
    "marts.sama_pilot_funnel",
    "marts.sama_pilot_order_changes",
    "marts.sama_pilot_native_economics",
    "marts.sama_pilot_data_quality",
    "marts.sama_pilot_tiktok_native_performance",
    "marts.sama_pilot_tiktok_campaign_outcomes",
    "marts.sama_pilot_tiktok_data_quality",
    "marts.sama_pilot_unified_overview",
    "marts.sama_pilot_unified_daily",
    "marts.sama_pilot_unified_native_economics",
    "marts.sama_pilot_business_leakage",
    "marts.sama_pilot_campaign_diagnostics",
    "marts.sama_pilot_intelligence_signals",
}


class MetabaseConfigurationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.compose = yaml.safe_load(
            (PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        )

    def test_metabase_image_and_host_port_are_pinned(self) -> None:
        service = self.compose["services"]["metabase"]
        self.assertEqual(service["image"], "metabase/metabase:v0.63.16.5")
        self.assertIn("127.0.0.1:3000:3000", service["ports"])
        self.assertIn("/api/health", " ".join(service["healthcheck"]["test"]))
        self.assertEqual(service["environment"]["MB_AI_FEATURES_ENABLED"], "false")
        self.assertEqual(service["environment"]["MB_LOAD_SAMPLE_CONTENT"], "false")
        self.assertIn("-Xmx512m", service["environment"]["JAVA_OPTS"])

    def test_metabase_metadata_is_physically_separate(self) -> None:
        services = self.compose["services"]
        self.assertIn("metabase-postgres", services)
        self.assertEqual(services["metabase"]["environment"]["MB_DB_HOST"], "metabase-postgres")
        self.assertNotEqual(
            services["metabase"]["environment"]["MB_DB_DBNAME"],
            services["warehouse-postgres"]["environment"]["POSTGRES_DB"],
        )
        self.assertIn("metabase-postgres-data", self.compose["volumes"])

    def test_analytics_connection_targets_only_the_warehouse(self) -> None:
        environment = self.compose["services"]["metabase-setup"]["environment"]
        self.assertEqual(environment["METABASE_WAREHOUSE_HOST"], "warehouse-postgres")
        self.assertEqual(environment["METABASE_WAREHOUSE_PORT"], 5432)
        self.assertNotIn("airflow-postgres", str(environment))

    def test_bi_queries_reference_all_and_only_dbt_marts(self) -> None:
        query_text = "\n".join(
            path.read_text(encoding="utf-8").lower()
            for directory in (
                "queries", "marketing_queries", "operations_queries",
                "economics_queries", "olist_queries", "uci_queries",
                "sama_pilot_queries", "sama_tiktok_queries",
                "sama_unified_queries",
                "sama_intelligence_queries",
            )
            for path in (PROJECT_ROOT / "bi" / directory).glob("*.sql")
        )
        for mart in EXPECTED_MARTS:
            self.assertIn(mart, query_text)
        self.assertNotIn("analytics.", query_text)
        self.assertNotIn("bronze", query_text)
        self.assertNotIn("silver", query_text)

    def test_revenue_queries_keep_currency_explicit(self) -> None:
        for name in ("revenue_overview.sql", "revenue_trend.sql"):
            query = (PROJECT_ROOT / "bi" / "queries" / name).read_text(
                encoding="utf-8"
            ).lower()
            self.assertIn("currency", query)

    def test_every_marketplace_query_preserves_business_context(self) -> None:
        for path in (PROJECT_ROOT / "bi" / "queries").glob("*.sql"):
            self.assertIn("business_id", path.read_text(encoding="utf-8").lower(), path.name)

    def test_marketing_dashboard_is_separate_and_uses_explicit_attribution_labels(self) -> None:
        source = (PROJECT_ROOT / "bi" / "setup_metabase.py").read_text(encoding="utf-8")
        self.assertIn("Pulse Marketing Performance", source)
        self.assertIn("Platform-reported conversions", source)
        self.assertIn("Platform-reported ROAS", source)
        self.assertIn('(\"business\", \"platform\", \"campaign\", \"currency\", \"start_date\", \"end_date\")', source)

    def test_operations_dashboard_is_separate_and_uses_precise_financial_language(self) -> None:
        source = (PROJECT_ROOT / "bi" / "setup_metabase.py").read_text(encoding="utf-8")
        self.assertIn("Pulse Commerce Operations", source)
        for label in ("Delivered orders", "COD cash collected", "Net remitted",
                      "Confirmation rate", "Delivery rate"):
            self.assertIn(label, source)
        queries = list((PROJECT_ROOT / "bi" / "operations_queries").glob("*.sql"))
        self.assertEqual(len(queries), 14)
        for path in queries:
            text = path.read_text().lower()
            self.assertIn("business_id", text)
            self.assertNotIn(" profit", text)

    def test_lifetime_rankings_do_not_expose_mixed_currency_money(self) -> None:
        for name in ("top_customers.sql", "top_products.sql"):
            query = (PROJECT_ROOT / "bi" / "queries" / name).read_text().lower()
            self.assertNotIn("revenue", query)
            self.assertIn("purchase_rank", query)

    def test_economics_dashboard_is_separate_and_uses_contribution_language(self) -> None:
        source = (PROJECT_ROOT / "bi" / "setup_metabase.py").read_text(encoding="utf-8")
        self.assertIn("Pulse Commerce Economics", source)
        for label in ("Product COGS", "Variable operational costs",
                      "Contribution before marketing", "Contribution after marketing",
                      "Attributed campaign economics", "Incomplete economics / missing COGS"):
            self.assertIn(label, source)
        queries = list((PROJECT_ROOT / "bi" / "economics_queries").glob("*.sql"))
        self.assertEqual(len(queries), 16)
        for path in queries:
            text = path.read_text(encoding="utf-8").lower()
            self.assertIn("business_id", text)
            self.assertNotIn("net_profit", text)

    def test_olist_dashboard_is_separate_and_business_filtered(self) -> None:
        source = (PROJECT_ROOT / "bi" / "setup_metabase.py").read_text(encoding="utf-8")
        self.assertIn("Pulse Olist Benchmark", source)
        queries = list((PROJECT_ROOT / "bi" / "olist_queries").glob("*.sql"))
        self.assertEqual(len(queries), 5)
        for path in queries:
            self.assertIn("business_id", path.read_text(encoding="utf-8").lower())

    def test_uci_dashboard_is_small_separate_and_uses_ledger_language(self) -> None:
        source = (PROJECT_ROOT / "bi" / "setup_metabase.py").read_text(encoding="utf-8")
        self.assertIn("Pulse UCI Retail Benchmark", source)
        self.assertIn("Daily signed ledger values", source)
        queries = list((PROJECT_ROOT / "bi" / "uci_queries").glob("*.sql"))
        self.assertEqual(len(queries), 8)
        for path in queries:
            text = path.read_text(encoding="utf-8").lower()
            self.assertIn("business_id", text)
            self.assertNotIn("net_profit", text)

    def test_sama_dashboard_is_aggregate_only_and_keeps_currencies_separate(self) -> None:
        source = (PROJECT_ROOT / "bi" / "setup_metabase.py").read_text(encoding="utf-8")
        self.assertIn("Pulse — Real COD Pilot", source)
        self.assertIn("FX REQUIRED — Economic Completeness", source)
        self.assertIn("Cross-currency contribution is intentionally unavailable", source)
        queries = list((PROJECT_ROOT / "bi" / "sama_pilot_queries").glob("*.sql"))
        self.assertEqual(len(queries), 14)
        combined = "\n".join(path.read_text(encoding="utf-8").lower() for path in queries)
        self.assertIn("collected_native_currency", combined)
        self.assertIn("known_operational_cost_usd", combined)
        self.assertNotIn("profit", combined)
        self.assertNotIn("total_usd", combined)
        for path in queries:
            text = path.read_text(encoding="utf-8").lower()
            self.assertIn("business_id", text)
            self.assertNotIn("phone", text)
            self.assertNotIn("customer", text)

        funnel = (PROJECT_ROOT / "bi" / "sama_pilot_queries" / "funnel.sql").read_text(
            encoding="utf-8"
        ).lower()
        self.assertIn("stage_name", funnel)
        self.assertIn("order_count", funnel)
        self.assertNotIn("confirmation_rate", funnel)
        self.assertNotIn("delivery_rate", funnel)
        self.assertNotIn("return_rate", funnel)

        native_revenue = (
            PROJECT_ROOT / "bi" / "sama_pilot_queries" / "native_revenue.sql"
        ).read_text(encoding="utf-8").lower()
        self.assertIn("group by business_id, currency", native_revenue)
        self.assertNotIn("delivered_orders", native_revenue)

        usd_costs = (PROJECT_ROOT / "bi" / "sama_pilot_queries" / "usd_costs.sql").read_text(
            encoding="utf-8"
        ).lower()
        for label in ("product cogs", "call center", "logistics", "known operational cost"):
            self.assertIn(label, usd_costs)
        self.assertNotIn("currency,", usd_costs)

    def test_sama_tiktok_dashboard_separates_platform_and_business_metrics(self) -> None:
        source = (PROJECT_ROOT / "bi" / "setup_metabase.py").read_text(encoding="utf-8")
        self.assertIn("Pulse — Real TikTok Performance", source)
        self.assertIn("TikTok-reported conversions; not labeled as observed orders", source)
        self.assertIn("Observed outcomes stop at campaign grain", source)
        self.assertIn("Platform vs Observed Business Outcomes", source)
        self.assertIn('"previous_title": "Platform vs Observed Business Funnel"', source)
        self.assertIn("Delivered and Returned are sibling terminal outcomes", source)
        self.assertIn('"scalar.field": "target_campaign_spend_display"', source)
        queries = list((PROJECT_ROOT / "bi" / "sama_tiktok_queries").glob("*.sql"))
        self.assertEqual(len(queries), 11)
        combined = "\n".join(path.read_text(encoding="utf-8").lower() for path in queries)
        self.assertIn("platform_conversions", combined)
        self.assertIn("lightfunnels_orders", combined)
        self.assertIn("fx_required", combined)
        self.assertNotIn("business_roas", combined)
        self.assertNotIn("platform_conversion_value", combined)
        self.assertIn("target_campaign_spend_display", combined)
        self.assertIn("delivered orders (terminal outcome)", combined)
        self.assertIn("returned orders (terminal outcome)", combined)
        for path in queries:
            text = path.read_text(encoding="utf-8").lower()
            self.assertIn("business_id", text)
            for forbidden in ("phone", "email", "address", "tracking"):
                self.assertNotIn(forbidden, text)

    def test_sama_unified_dashboard_uses_exact_and_non_scroll_executive_cards(self) -> None:
        source = (PROJECT_ROOT / "bi" / "setup_metabase.py").read_text(encoding="utf-8")
        self.assertIn('"scalar.compact_primary_number"] = False', source)
        self.assertIn('scalar_settings("target_spend_usd", money=True, exact=True)', source)
        for title in (
            "Confirmation Rate",
            "Delivery Rate",
            "Efficiency Return Rate",
            "Cost / Lightfunnels Order",
            "Cost / Confirmed Order",
            "Efficiency Marketing Cost / Delivered",
        ):
            self.assertIn(f'"title": "{title}"', source)
        self.assertIn('"previous_title": "Efficiency & Conversion"', source)
        self.assertIn('"card.title": "Return Rate"', source)
        self.assertIn('"card.title": "Marketing Cost / Delivered"', source)
        self.assertIn('"display": "text"', source)
        self.assertIn('"## FX_REQUIRED\\n\\n"', source)
        self.assertIn("Marketing and known operating costs are USD.", source)
        self.assertIn("COD collections are retained in native currencies.", source)
        self.assertIn(
            "Cross-currency profit, contribution, margin and business ROAS are unavailable",
            source,
        )

    def test_sama_intelligence_priority_tables_are_compact_and_separate(self) -> None:
        source = (PROJECT_ROOT / "bi" / "setup_metabase.py").read_text(encoding="utf-8")
        priority = (PROJECT_ROOT / "bi" / "sama_intelligence_queries" /
                    "priority_signals.sql").read_text(encoding="utf-8").lower()
        investigations = (PROJECT_ROOT / "bi" / "sama_intelligence_queries" /
                          "recommended_investigations.sql").read_text(encoding="utf-8").lower()

        self.assertIn('"title": "Priority Signals"', source)
        self.assertIn('"columns": "priority, signal, scope, impact, confidence"', source)
        self.assertIn('"title": "Recommended Investigations"', source)
        self.assertIn('"columns": "signal, scope, recommended_investigation"', source)
        self.assertNotIn(
            '"columns": "priority, signal, scope, evidence, benchmark_gap_orders, confidence, recommended_investigation"',
            source,
        )

        for field in ("signal_order", "priority", "signal_type", "scope_name",
                      "impact_order_count", "confidence"):
            self.assertIn(field, priority)
        self.assertNotIn("evidence_summary", priority)
        self.assertNotIn("recommended_next_step", priority)

        for field in ("signal_order", "signal_type", "scope_name", "recommended_next_step"):
            self.assertIn(field, investigations)
        self.assertNotIn("evidence_summary", investigations)


class MetabaseProvisioningTests(unittest.TestCase):
    def test_duplicate_names_fail_instead_of_selecting_arbitrarily(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "Multiple objects"):
            setup._unique([{"name": "Pulse"}, {"name": "Pulse"}], "Pulse")

    def test_unavailable_create_is_not_retried(self) -> None:
        with patch.object(setup.urllib.request, "urlopen", side_effect=OSError("offline")) as request:
            with self.assertRaisesRegex(RuntimeError, "unavailable after 1 attempts"):
                setup._request("POST", "/api/collection", {"name": "Pulse"})
            self.assertEqual(request.call_count, 1)

    def test_unavailable_read_fails_after_bounded_retries(self) -> None:
        with patch.object(setup.urllib.request, "urlopen", side_effect=OSError("offline")) as request, patch.object(setup.time, "sleep"):
            with self.assertRaisesRegex(RuntimeError, "unavailable after 5 attempts"):
                setup._request("GET", "/api/session/properties")
            self.assertEqual(request.call_count, 5)

    def test_wrong_warehouse_target_is_rejected(self) -> None:
        with patch.object(setup, "_databases", return_value=[{
            "id": 1, "name": setup.WAREHOUSE_NAME, "details": {"host": "airflow-postgres"}
        }]):
            with self.assertRaisesRegex(RuntimeError, "unexpected host"):
                setup._ensure_warehouse("session")


if __name__ == "__main__":
    unittest.main()
