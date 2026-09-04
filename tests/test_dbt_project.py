"""Static contracts for the Pulse dbt project and lineage."""

from __future__ import annotations

import unittest
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_ROOT = PROJECT_ROOT / "dbt"
EXPECTED_SOURCES = {
    "daily_sales",
    "customer_metrics",
    "product_metrics",
    "funnel_metrics",
}
EXPECTED_MARTS = {
    "revenue_by_day",
    "top_customers",
    "top_products",
    "funnel_performance",
}


class DbtProjectContractTests(unittest.TestCase):
    def test_project_targets_marts_as_views(self) -> None:
        project = yaml.safe_load((DBT_ROOT / "dbt_project.yml").read_text(encoding="utf-8"))
        self.assertEqual(project["profile"], "pulse_analytics")
        self.assertEqual(
            project["models"]["pulse_analytics"]["marts"]["+materialized"],
            "view",
        )

    def test_profile_uses_environment_variables(self) -> None:
        profile = (DBT_ROOT / "profiles.yml").read_text(encoding="utf-8")
        for variable in (
            "WAREHOUSE_HOST",
            "WAREHOUSE_PORT",
            "WAREHOUSE_DB",
            "WAREHOUSE_USER",
            "WAREHOUSE_PASSWORD",
            "DBT_SCHEMA",
        ):
            self.assertIn(f"env_var('{variable}'", profile)
        self.assertIn("env_var('WAREHOUSE_PASSWORD')", profile)
        self.assertNotIn("env_var('WAREHOUSE_PASSWORD',", profile)
        self.assertNotIn("airflow-postgres", profile)

    def test_source_table_names_are_exact(self) -> None:
        source_config = yaml.safe_load(
            (DBT_ROOT / "models" / "sources.yml").read_text(encoding="utf-8")
        )
        source = source_config["sources"][0]
        self.assertEqual(source["name"], "analytics")
        self.assertEqual(source["schema"], "analytics")
        self.assertEqual({table["name"] for table in source["tables"]}, EXPECTED_SOURCES)

    def test_expected_marts_exist_and_reference_only_warehouse_sources(self) -> None:
        marts_dir = DBT_ROOT / "models" / "marts"
        self.assertEqual({path.stem for path in marts_dir.glob("*.sql")}, EXPECTED_MARTS)
        sql = "\n".join(path.read_text(encoding="utf-8") for path in marts_dir.glob("*.sql"))
        for source in EXPECTED_SOURCES:
            self.assertIn(f"source('analytics', '{source}')", sql)
        self.assertNotIn("ref(", sql)

    def test_revenue_mart_preserves_currency_grain(self) -> None:
        sql = (DBT_ROOT / "models" / "marts" / "revenue_by_day.sql").read_text(
            encoding="utf-8"
        ).lower()
        self.assertIn("currency", sql)
        self.assertIn("group by event_date, currency", sql)


if __name__ == "__main__":
    unittest.main()
