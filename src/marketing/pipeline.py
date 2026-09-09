"""Deterministic Bronze/Silver/Gold performance-marketing pipeline."""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
import json
import os
from pathlib import Path
import sys
from typing import Any, Iterable

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType, DoubleType, LongType, StringType, StructField, StructType, TimestampType,
)

from src.marketing.adapters import MARKETING_ADAPTERS, marketing_adapter_for
from src.marketing.models import kpis
from src.onboarding.models import IngestionEnvelope
from src.onboarding.registry import BusinessRegistry, validate_business
from src.streaming.windows_spark import configure_windows_spark_builder, configure_windows_spark_environment


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MARKETING_GOLD_TABLES = (
    "marketing_daily", "campaign_performance", "ad_group_performance", "ad_performance",
)


@dataclass(frozen=True, slots=True)
class MarketingPaths:
    bronze: Path
    silver: Path
    marketing_daily: Path
    campaign_performance: Path
    ad_group_performance: Path
    ad_performance: Path


def _path(value: str, root: Path) -> Path:
    candidate = Path(value)
    return candidate.resolve() if candidate.is_absolute() else (root / candidate).resolve()


def load_marketing_paths(environ=None, *, project_root: Path = PROJECT_ROOT) -> MarketingPaths:
    environment = os.environ if environ is None else environ
    return MarketingPaths(
        bronze=_path(environment.get("BRONZE_MARKETING_PATH", "data/bronze/marketing_daily"), project_root),
        silver=_path(environment.get("SILVER_MARKETING_PATH", "data/silver/marketing_daily"), project_root),
        marketing_daily=_path(environment.get("GOLD_MARKETING_DAILY_PATH", "data/gold/marketing_daily"), project_root),
        campaign_performance=_path(environment.get("GOLD_CAMPAIGN_PERFORMANCE_PATH", "data/gold/campaign_performance"), project_root),
        ad_group_performance=_path(environment.get("GOLD_AD_GROUP_PERFORMANCE_PATH", "data/gold/ad_group_performance"), project_root),
        ad_performance=_path(environment.get("GOLD_AD_PERFORMANCE_PATH", "data/gold/ad_performance"), project_root),
    )


def build_marketing_spark_session(*, app_name="pulse-marketing", master="local[*]") -> SparkSession:
    configure_windows_spark_environment()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (SparkSession.builder.appName(app_name).master(master)
               .config("spark.ui.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", os.getenv("MARKETING_SHUFFLE_PARTITIONS", "4")))
    return configure_windows_spark_builder(builder).getOrCreate()


BRONZE_SCHEMA = StructType([
    StructField("business_id", StringType(), False), StructField("source_type", StringType(), False),
    StructField("source_id", StringType(), False), StructField("ingestion_id", StringType(), False),
    StructField("record_id", StringType(), False), StructField("extracted_at_utc", TimestampType(), False),
    StructField("source_updated_at_utc", TimestampType(), True), StructField("schema_version", StringType(), False),
    StructField("payload", StringType(), False),
])

SILVER_COLUMNS = (
    "business_id", "source_type", "source_id", "ingestion_id", "record_id",
    "extracted_at_utc", "source_updated_at_utc", "schema_version", "grain", "platform",
    "account_id", "campaign_id", "campaign_name", "ad_group_id", "ad_group_name",
    "ad_id", "ad_name", "creative_id", "report_date", "reporting_timezone", "currency",
    "spend", "impressions", "reach", "frequency", "clicks", "link_clicks",
    "platform_conversions", "platform_conversion_value", "video_views", "landing_page_views",
    "details_json",
)

# Explicit construction keeps nullable/type contracts reviewable without relying on inference.
SILVER_SCHEMA = StructType([
    StructField("business_id", StringType(), False), StructField("source_type", StringType(), False),
    StructField("source_id", StringType(), False), StructField("ingestion_id", StringType(), False),
    StructField("record_id", StringType(), False), StructField("extracted_at_utc", TimestampType(), False),
    StructField("source_updated_at_utc", TimestampType(), True), StructField("schema_version", StringType(), False),
    StructField("grain", StringType(), False), StructField("platform", StringType(), False),
    StructField("account_id", StringType(), False), StructField("campaign_id", StringType(), False),
    StructField("campaign_name", StringType(), True), StructField("ad_group_id", StringType(), True),
    StructField("ad_group_name", StringType(), True), StructField("ad_id", StringType(), True),
    StructField("ad_name", StringType(), True), StructField("creative_id", StringType(), True),
    StructField("report_date", DateType(), False), StructField("reporting_timezone", StringType(), False),
    StructField("currency", StringType(), False), StructField("spend", DoubleType(), False),
    StructField("impressions", LongType(), False), StructField("reach", LongType(), True),
    StructField("frequency", DoubleType(), True), StructField("clicks", LongType(), False),
    StructField("link_clicks", LongType(), True), StructField("platform_conversions", DoubleType(), False),
    StructField("platform_conversion_value", DoubleType(), False), StructField("video_views", LongType(), True),
    StructField("landing_page_views", LongType(), True), StructField("details_json", StringType(), False),
])

METRIC_FIELDS = (
    "spend", "impressions", "clicks", "platform_conversions", "platform_conversion_value",
    "link_clicks", "video_views", "landing_page_views",
)
KPI_FIELDS = ("ctr", "cpc", "cpm", "cpa", "platform_roas")


def _gold_schema(id_fields: tuple[str, ...]) -> StructType:
    optional_names = {"campaign_name", "ad_group_name", "ad_name", "creative_id",
                      "link_clicks", "video_views", "landing_page_views"}
    fields = [
        StructField("business_id", StringType(), False), StructField("source_type", StringType(), False),
        StructField("source_id", StringType(), False), StructField("platform", StringType(), False),
        StructField("account_id", StringType(), False),
    ]
    for name in id_fields:
        fields.append(StructField(name, StringType(), name in optional_names))
    fields.extend([
        StructField("report_date", DateType(), False), StructField("reporting_timezone", StringType(), False),
        StructField("currency", StringType(), False), StructField("spend", DoubleType(), False),
        StructField("impressions", LongType(), False), StructField("clicks", LongType(), False),
        StructField("platform_conversions", DoubleType(), False),
        StructField("platform_conversion_value", DoubleType(), False),
        StructField("link_clicks", LongType(), True), StructField("video_views", LongType(), True),
        StructField("landing_page_views", LongType(), True),
        *(StructField(name, DoubleType(), True) for name in KPI_FIELDS),
    ])
    return StructType(fields)


GOLD_ID_FIELDS = {
    "marketing_daily": (),
    "campaign_performance": ("campaign_id", "campaign_name"),
    "ad_group_performance": ("campaign_id", "campaign_name", "ad_group_id", "ad_group_name"),
    "ad_performance": ("campaign_id", "campaign_name", "ad_group_id", "ad_group_name",
                       "ad_id", "ad_name", "creative_id"),
}
GOLD_SCHEMAS = {name: _gold_schema(fields) for name, fields in GOLD_ID_FIELDS.items()}


def extract_registered_marketing(registry: BusinessRegistry | None = None,
                                 *, business_id: str | None = None,
                                 source_id: str | None = None) -> tuple[tuple[Any, tuple[IngestionEnvelope, ...]], ...]:
    selected = registry or BusinessRegistry()
    business_ids = [business_id] if business_id else [b.business_id for b in selected.list_businesses()
                                                  if b.status == "ACTIVE"]
    output = []
    for owner in business_ids:
        report = validate_business(selected, owner)
        if not report.valid:
            raise ValueError(f"Invalid onboarding for {owner}")
        enabled = set(selected.get_business(owner).enabled_sources)
        for config in selected.sources_for(owner):
            if config.enabled and config.source_id in enabled and config.source_type in MARKETING_ADAPTERS:
                if source_id is not None and config.source_id != source_id:
                    continue
                adapter = marketing_adapter_for(config)
                output.append((adapter, adapter.extract()))
    if source_id is not None and not output:
        raise ValueError(f"Enabled marketing source {source_id!r} was not found")
    return tuple(output)


def normalize_latest(extractions: Iterable[tuple[Any, Iterable[IngestionEnvelope]]]) -> tuple[dict[str, Any], ...]:
    """Normalize and keep the latest source revision for each logical daily grain."""
    latest: dict[tuple[str, ...], tuple[tuple[datetime, datetime], dict[str, Any]]] = {}
    for adapter, envelopes in extractions:
        for envelope in envelopes:
            row = adapter.normalize(envelope)
            identity = tuple(row[name] or "" for name in (
                "business_id", "source_type", "source_id", "schema_version", "platform", "account_id",
                "campaign_id", "ad_group_id", "ad_id", "report_date",
            ))
            stamp = (envelope.source_updated_at_utc or envelope.extracted_at_utc, envelope.extracted_at_utc)
            if identity not in latest or stamp >= latest[identity][0]:
                latest[identity] = (stamp, row)
    return tuple(value[1] for _, value in sorted(latest.items()))


def _sum_optional(rows, name):
    values = [row[name] for row in rows if row.get(name) is not None]
    return None if not values else sum(values)


def _aggregate(rows: tuple[dict[str, Any], ...], id_fields: tuple[str, ...]) -> tuple[dict[str, Any], ...]:
    grain_fields = tuple(name for name in id_fields if name.endswith("_id") and name != "creative_id")
    attribute_fields = tuple(name for name in id_fields if name not in grain_fields)
    key_fields = ("business_id", "source_type", "source_id", "platform", "account_id", *grain_fields,
                  "report_date", "reporting_timezone", "currency")
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row.get(name) for name in key_fields)].append(row)
    output = []
    for key, members in sorted(groups.items()):
        result = dict(zip(key_fields, key))
        result.update({name: next((row.get(name) for row in reversed(members) if row.get(name)), None)
                       for name in attribute_fields})
        result.update(
            spend=sum(row["spend"] for row in members),
            impressions=sum(row["impressions"] for row in members),
            clicks=sum(row["clicks"] for row in members),
            platform_conversions=sum(row["platform_conversions"] for row in members),
            platform_conversion_value=sum(row["platform_conversion_value"] for row in members),
            link_clicks=_sum_optional(members, "link_clicks"),
            video_views=_sum_optional(members, "video_views"),
            landing_page_views=_sum_optional(members, "landing_page_views"),
        )
        result.update(kpis(**{name: result[name] for name in (
            "spend", "impressions", "clicks", "platform_conversions", "platform_conversion_value"
        )}))
        output.append(result)
    return tuple(output)


def build_gold_rows(silver: Iterable[dict[str, Any]]) -> dict[str, tuple[dict[str, Any], ...]]:
    rows = tuple(silver)
    return {name: _aggregate(rows, fields) for name, fields in GOLD_ID_FIELDS.items()}


def _silver_spark_row(row):
    output = dict(row)
    output["report_date"] = date.fromisoformat(output["report_date"])
    output["extracted_at_utc"] = datetime.fromisoformat(output["extracted_at_utc"].replace("Z", "+00:00"))
    output["source_updated_at_utc"] = (datetime.fromisoformat(output["source_updated_at_utc"].replace("Z", "+00:00"))
                                               if output["source_updated_at_utc"] else None)
    output["details_json"] = json.dumps(output.pop("details"), sort_keys=True, separators=(",", ":"))
    return tuple(output[name] for name in SILVER_COLUMNS)


def run_pipeline(registry: BusinessRegistry | None = None, *, business_id: str | None = None,
                 source_id: str | None = None, paths: MarketingPaths | None = None,
                 spark: SparkSession | None = None) -> dict[str, int]:
    selected_paths = paths or load_marketing_paths()
    extractions = extract_registered_marketing(registry, business_id=business_id, source_id=source_id)
    envelopes = tuple(envelope for _, batch in extractions for envelope in batch)
    silver = normalize_latest(extractions)
    gold = build_gold_rows(silver)
    own_spark = spark is None
    session = spark or build_marketing_spark_session(master=os.getenv("SPARK_MASTER", "local[*]"))
    try:
        bronze_rows = [(
            e.business_id, e.source_type, e.source_id, e.ingestion_id, e.record_id,
            e.extracted_at_utc, e.source_updated_at_utc, e.schema_version,
            json.dumps(e.payload, sort_keys=True, separators=(",", ":")),
        ) for e in envelopes]
        session.createDataFrame(bronze_rows, BRONZE_SCHEMA).write.mode("overwrite").parquet(str(selected_paths.bronze))
        session.createDataFrame([_silver_spark_row(row) for row in silver], SILVER_SCHEMA).write.mode("overwrite").parquet(str(selected_paths.silver))
        for name, rows in gold.items():
            schema = GOLD_SCHEMAS[name]
            values = []
            for row in rows:
                converted = {**row, "report_date": date.fromisoformat(row["report_date"])}
                values.append(tuple(converted.get(field.name) for field in schema.fields))
            session.createDataFrame(values, schema).write.mode("overwrite").parquet(str(getattr(selected_paths, name)))
    finally:
        if own_spark:
            session.stop()
    return {"bronze": len(envelopes), "silver": len(silver),
            **{name: len(rows) for name, rows in gold.items()}}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "demo"))
    parser.add_argument("--business-id")
    args = parser.parse_args(argv)
    if args.command == "demo":
        extractions = extract_registered_marketing(business_id=args.business_id)
        silver = normalize_latest(extractions)
        result = {"bronze": sum(len(batch) for _, batch in extractions), "silver": len(silver),
                  **{name: len(rows) for name, rows in build_gold_rows(silver).items()}}
    else:
        result = run_pipeline(business_id=args.business_id)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
