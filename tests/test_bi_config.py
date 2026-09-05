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
            for path in (PROJECT_ROOT / "bi" / "queries").glob("*.sql")
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

    def test_lifetime_rankings_do_not_expose_mixed_currency_money(self) -> None:
        for name in ("top_customers.sql", "top_products.sql"):
            query = (PROJECT_ROOT / "bi" / "queries" / name).read_text().lower()
            self.assertNotIn("revenue", query)
            self.assertIn("purchase_rank", query)


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
