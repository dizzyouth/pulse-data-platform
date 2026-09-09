"""Focused Phase 6.1 marketing contracts, normalization, isolation, and flow tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from uuid import NAMESPACE_URL, uuid5

from src.marketing.adapters import (
    MARKETING_ADAPTERS, MarketingSourceAdapter, reporting_start,
)
from src.marketing.models import MarketingGrain, MarketingRecord, kpis
from src.marketing.pipeline import (
    GOLD_SCHEMAS, MarketingPaths, _silver_spark_row, build_gold_rows,
    extract_registered_marketing, normalize_latest, run_pipeline,
)
from src.onboarding.contracts import SourceContractError, contract_for
from src.onboarding.registry import BusinessRegistry
from src.quality.anomaly import AnomalyPolicy, AnomalyStatus, MetricSeries, evaluate
from src.quality.datasets import marketing_gold_rules, marketing_silver_rules
from src.quality.models import QualityContext, Status
from src.quality.runner import run_quality_checks
from src.warehouse.load_marketing import MARKETING_GRAINS, MARKETING_TABLE_SPECS


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 1, 7, 12, tzinfo=timezone.utc)


def canonical(**updates):
    values = dict(
        business_id="business_a", source_type="meta_ads", source_id="ads_primary",
        ingestion_id="ingestion", record_id="record", extracted_at_utc=NOW,
        source_updated_at_utc=NOW, schema_version="meta_ads_daily_v1", platform="meta_ads",
        account_id="account", campaign_id="123", campaign_name="Campaign",
        ad_group_id="group", ad_group_name="Group", ad_id="ad", ad_name="Ad",
        creative_id=None, report_date=date(2026, 1, 4), reporting_timezone="America/New_York",
        currency="USD", spend=10.0, impressions=100, reach=80, frequency=1.25,
        clicks=5, link_clicks=4, platform_conversions=2.0,
        platform_conversion_value=30.0, video_views=None, landing_page_views=None,
        details={"native": "retained"},
    )
    values.update(updates)
    return MarketingRecord(**values)


class MarketingModelTests(unittest.TestCase):
    def test_canonical_grains_are_explicit_and_optional_metrics_remain_optional(self):
        self.assertEqual(canonical().grain, MarketingGrain.AD_DAILY)
        self.assertEqual(canonical(ad_id=None).grain, MarketingGrain.AD_GROUP_DAILY)
        self.assertEqual(canonical(ad_id=None, ad_group_id=None).grain, MarketingGrain.CAMPAIGN_DAILY)
        self.assertIsNone(canonical(reach=None, frequency=None).reach)

    def test_invalid_identity_currency_timezone_metrics_and_click_relationship_fail(self):
        for updates in ({"business_id": "Business A"}, {"currency": "usd"},
                        {"reporting_timezone": "Mars/Olympus"}, {"spend": -1},
                        {"clicks": 101}, {"ad_group_id": None}):
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                canonical(**updates)

    def test_kpis_use_raw_sums_and_zero_denominators_are_null(self):
        self.assertEqual(kpis(spend=10, impressions=100, clicks=5,
                              platform_conversions=2, platform_conversion_value=30),
                         {"ctr": 0.05, "cpc": 2.0, "cpm": 100.0,
                          "cpa": 5.0, "platform_roas": 3.0})
        self.assertEqual(kpis(spend=0, impressions=0, clicks=0,
                              platform_conversions=0, platform_conversion_value=0),
                         {"ctr": None, "cpc": None, "cpm": None,
                          "cpa": None, "platform_roas": None})


class MarketingAdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = BusinessRegistry(ROOT / "config" / "businesses", ROOT / "config" / "sources")
        cls.extractions = extract_registered_marketing(cls.registry)
        cls.silver = normalize_latest(cls.extractions)

    def test_all_provider_contracts_are_versioned_ad_daily_contracts(self):
        versions = {
            "meta_ads": "meta_ads_daily_v1", "tiktok_ads": "tiktok_ads_daily_v1",
            "google_ads": "google_ads_daily_v1", "generic_ads": "generic_ads_daily_v1",
        }
        for source_type, version in versions.items():
            with self.subTest(source_type=source_type):
                contract = contract_for(source_type, version)
                self.assertEqual(contract.grain_name, "ad_daily")
                self.assertTrue(contract.metric_semantics)
                self.assertTrue(contract.date_fields)
        with self.assertRaises(SourceContractError):
            contract_for("google_ads", "google_ads_daily_v2")

    def test_every_provider_uses_existing_adapter_abstraction_and_normalizes_terms(self):
        seen = set()
        for adapter, envelopes in self.extractions:
            self.assertIsInstance(adapter, MarketingSourceAdapter)
            self.assertIn(adapter.config.source_type, MARKETING_ADAPTERS)
            self.assertTrue(adapter.healthcheck().healthy)
            for envelope in envelopes:
                row = adapter.normalize(envelope)
                seen.add(row["source_type"])
                self.assertIn("ad_group_id", row)
                self.assertNotIn("adset_id", row)
        self.assertEqual(seen, set(MARKETING_ADAPTERS))

    def test_bronze_envelope_keeps_full_native_payload_and_lineage(self):
        adapter, envelopes = next(item for item in self.extractions
                                  if item[0].config.source_type == "meta_ads")
        envelope = envelopes[0]
        self.assertIn("adset_id", envelope.payload)
        self.assertTrue(envelope.ingestion_id and envelope.record_id)
        self.assertIsNotNone(envelope.source_updated_at_utc)
        self.assertEqual(envelope.schema_version, "meta_ads_daily_v1")

    def test_business_platform_source_and_currency_isolation_survive_shared_ids(self):
        shared = [row for row in self.silver if row["campaign_id"] == "123"]
        self.assertEqual({row["business_id"] for row in shared},
                         {"pulse_demo_store", "marketing_demo_a", "marketing_demo_b"})
        self.assertEqual({row["platform"] for row in shared},
                         {"meta_ads", "tiktok_ads", "google_ads", "affiliate_network"})
        self.assertEqual({row["currency"] for row in shared}, {"USD", "GBP", "EUR"})
        self.assertEqual(len({(row["business_id"], row["source_id"], row["platform"], row["record_id"])
                              for row in self.silver}), len(self.silver))

    def test_late_attribution_reuses_record_identity_and_latest_revision_wins(self):
        adapter, envelopes = next(item for item in self.extractions
                                  if item[0].config.business_id == "marketing_demo_a"
                                  and item[0].config.source_type == "meta_ads")
        revisions = [row for row in envelopes if row.payload["date_start"] == "2026-01-04"]
        self.assertEqual(len(revisions), 2)
        self.assertEqual(len({row.record_id for row in revisions}), 1)
        latest = [row for row in self.silver if row["business_id"] == "marketing_demo_a"
                  and row["platform"] == "meta_ads" and row["report_date"] == "2026-01-04"]
        self.assertEqual(len(latest), 1)
        self.assertEqual(latest[0]["platform_conversions"], 3.0)
        self.assertEqual(normalize_latest(((adapter, tuple(reversed(envelopes))),)),
                         normalize_latest(((adapter, envelopes),)))

    def test_configurable_lookback_is_inclusive(self):
        self.assertEqual(reporting_start(date(2026, 1, 10), 3), date(2026, 1, 7))
        self.assertIsNone(reporting_start(None, 3))
        with self.assertRaises(ValueError):
            reporting_start(date(2026, 1, 10), -1)

    def test_gold_grains_and_ratio_of_sums_do_not_merge_platforms_or_currency(self):
        first = canonical().to_dict()
        second = canonical(record_id="record_2", ad_id="ad_2", ad_name="Ad 2",
                           spend=30, impressions=300, clicks=30,
                           platform_conversions=3, platform_conversion_value=60).to_dict()
        rows = build_gold_rows((first, second))
        campaign = rows["campaign_performance"]
        self.assertEqual(len(campaign), 1)
        self.assertEqual((campaign[0]["spend"], campaign[0]["ctr"], campaign[0]["cpc"]),
                         (40.0, 0.0875, 40.0 / 35.0))
        all_gold = build_gold_rows(self.silver)
        for name, values in all_gold.items():
            grain = MARKETING_GRAINS[name]
            self.assertEqual(len(values), len({tuple(row.get(column) for column in grain) for row in values}))

    def test_short_marketing_history_is_insufficient_not_anomalous(self):
        result = evaluate(MetricSeries(metric_name="daily_spend", dataset_name="marketing_daily",
            layer="analytics", current_value=20, history=(18,), observed_at_utc=NOW,
            history_observed_at_utc=(datetime(2026, 1, 6, tzinfo=timezone.utc),),
            dimensions={"business_id": "business_a", "source_type": "meta_ads",
                        "source_id": "ads_primary", "platform": "meta_ads"}),
            AnomalyPolicy(minimum_history=3), uuid5(NAMESPACE_URL, "marketing-test"))
        self.assertEqual(result.status, AnomalyStatus.INSUFFICIENT_HISTORY)

    def test_warehouse_contracts_match_all_gold_schemas(self):
        self.assertEqual({spec.name for spec in MARKETING_TABLE_SPECS}, set(GOLD_SCHEMAS))
        for spec in MARKETING_TABLE_SPECS:
            self.assertEqual(spec.required_columns, tuple(field.name for field in GOLD_SCHEMAS[spec.name].fields))
            self.assertIn("business_id", MARKETING_GRAINS[spec.name])
            self.assertIn("currency", MARKETING_GRAINS[spec.name])


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS", "1") == "1", "Spark tests disabled")
class MarketingSparkFlowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src.analytics.gold_build import build_gold_spark_session
        cls.spark = build_gold_spark_session(app_name="pulse-marketing-tests", master="local[1]")
        cls.spark.conf.set("spark.sql.shuffle.partitions", "1")
        cls.registry = BusinessRegistry(ROOT / "config" / "businesses", ROOT / "config" / "sources")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_quality_rules_and_synthetic_bronze_silver_gold_flow(self):
        extractions = extract_registered_marketing(self.registry)
        silver = normalize_latest(extractions)
        frame = self.spark.createDataFrame([_silver_spark_row(row) for row in silver],
                                           __import__("src.marketing.pipeline", fromlist=["SILVER_SCHEMA"]).SILVER_SCHEMA)
        results = run_quality_checks(frame, marketing_silver_rules(),
                                     QualityContext(dataset_name="marketing_silver", layer="silver"))
        self.assertTrue(all(result.status == Status.PASS for result in results))
        with TemporaryDirectory() as directory:
            root = Path(directory)
            paths = MarketingPaths(*(root / name for name in (
                "bronze", "silver", "marketing_daily", "campaign_performance",
                "ad_group_performance", "ad_performance")))
            counts = run_pipeline(self.registry, paths=paths, spark=self.spark)
            self.assertEqual(counts, {"bronze": 15, "silver": 12, "marketing_daily": 12,
                                     "campaign_performance": 12, "ad_group_performance": 12,
                                     "ad_performance": 12})
            bronze = self.spark.read.parquet(str(paths.bronze))
            self.assertEqual(bronze.count(), 15)
            self.assertIn("adset_id", json.loads(bronze.filter("source_type='meta_ads'").first().payload))
            for name in GOLD_SCHEMAS:
                gold = self.spark.read.schema(GOLD_SCHEMAS[name]).parquet(str(getattr(paths, name)))
                results = run_quality_checks(gold, marketing_gold_rules(name),
                    QualityContext(dataset_name=name, layer="gold"))
                self.assertTrue(all(result.status == Status.PASS for result in results), name)


class MarketingConfigurationTests(unittest.TestCase):
    def test_dbt_airflow_metabase_and_ci_are_offline_and_marketing_aware(self):
        dag = (ROOT / "airflow" / "dags" / "pulse_analytics_pipeline.py").read_text().lower()
        self.assertIn("src.marketing.pipeline build", dag)
        self.assertIn("marketing_warehouse", dag)
        workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text().lower()
        self.assertNotIn("meta_ads_token", workflow)
        self.assertNotIn("google_ads_token", workflow)
        setup = (ROOT / "bi" / "setup_metabase.py").read_text()
        self.assertIn("Pulse Marketing Performance", setup)
        for label in ("Platform-reported conversions", "Platform-reported ROAS"):
            self.assertIn(label, setup)
        queries = list((ROOT / "bi" / "marketing_queries").glob("*.sql"))
        self.assertEqual(len(queries), 8)
        for path in queries:
            text = path.read_text().lower()
            self.assertIn("business_id", text)
            self.assertIn("currency", text)
            self.assertNotIn("profit", text)


if __name__ == "__main__":
    unittest.main()
