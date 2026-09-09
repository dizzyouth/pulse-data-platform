"""Network-free synthetic onboarding proof across registry and adapters."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid5

from src.onboarding.adapters import adapter_for
from src.onboarding.registry import BusinessRegistry, validate_business
from src.quality.anomaly import AnomalyPolicy, MetricSeries, evaluate


@dataclass(frozen=True, slots=True, kw_only=True)
class DemoReport:
    business_id: str
    source_count: int
    bronze_records: int
    silver_records: int
    shopify_orders: int
    shopify_revenue: float
    meta_ads_spend: float
    source_record_counts: dict[str, int]
    gold_metrics: dict[str, float]
    warehouse_rows: int
    queried_business_rows: int
    quality_status: str
    anomaly_status: str


def run_demo(business_id="pulse_demo_store", registry=None):
    registry = registry or BusinessRegistry()
    validation = validate_business(registry, business_id)
    if not validation.valid:
        raise ValueError("Business onboarding configuration is invalid")
    bronze, silver = [], []
    counts = {}
    for config in registry.sources_for(business_id):
        if not config.enabled:
            continue
        adapter = adapter_for(config)
        records = adapter.extract()
        bronze.extend(record.to_dict() for record in records)
        silver.extend(adapter.normalize(record) for record in records)
        counts[config.source_id] = len(records)
    orders = [row for row in silver if row["source_type"] == "shopify"]
    ads = [row for row in silver if row["source_type"] == "meta_ads"]
    if any(row["business_id"] != business_id for row in silver):
        raise ValueError("Cross-business data detected in synthetic onboarding")
    gold_metrics = {
        "shopify_order_count": float(len(orders)),
        "shopify_gross_revenue": sum(float(row["total_amount"]) for row in orders),
        "meta_ads_spend": sum(float(row["spend"]) for row in ads),
        "manual_event_count": float(sum(row["source_type"] == "csv_manual" for row in silver)),
    }
    # A local, in-memory serving boundary keeps this command network-free while
    # exercising the same mandatory business grain used by PostgreSQL outputs.
    warehouse = tuple(
        {"business_id": business_id, "metric_name": name, "metric_value": value}
        for name, value in sorted(gold_metrics.items())
    )
    queried = tuple(row for row in warehouse if row["business_id"] == business_id)
    quality_status = "PASS" if len(queried) == len(warehouse) and warehouse else "FAIL"
    anomaly = evaluate(
        MetricSeries(
            metric_name="shopify_gross_revenue", dataset_name="onboarding_demo",
            layer="analytics", current_value=float(orders[-1]["total_amount"]),
            history=(float(orders[0]["total_amount"]),),
            observed_at_utc=datetime(2026, 1, 5, tzinfo=timezone.utc),
            history_observed_at_utc=(datetime(2026, 1, 4, tzinfo=timezone.utc),),
            dimensions={"business_id": business_id, "source_type": "shopify",
                         "source_id": next(row["source_id"] for row in orders)},
        ),
        AnomalyPolicy(minimum_history=3),
        uuid5(NAMESPACE_URL, f"onboarding-demo|{business_id}"),
    )
    return DemoReport(business_id=business_id, source_count=validation.source_count,
                      bronze_records=len(bronze), silver_records=len(silver),
                      shopify_orders=len(orders),
                      shopify_revenue=sum(float(row["total_amount"]) for row in orders),
                      meta_ads_spend=sum(float(row["spend"]) for row in ads),
                      source_record_counts=counts, gold_metrics=gold_metrics,
                      warehouse_rows=len(warehouse), queried_business_rows=len(queried),
                      quality_status=quality_status, anomaly_status=anomaly.status.value)
