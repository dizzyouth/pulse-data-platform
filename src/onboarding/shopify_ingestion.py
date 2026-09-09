"""Checkpoint-safe Shopify-to-Pulse Bronze ingestion and event projection."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Protocol
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

from src.onboarding.connector_state import ConnectorStateStore
from src.onboarding.models import BusinessConfig, IngestionEnvelope, SourceConfig
from src.onboarding.shopify import ShopifyAdapterError, ShopifyAdminApiAdapter


RECOGNIZED_FINANCIAL_STATUSES = {
    "PAID", "PARTIALLY_PAID", "PARTIALLY_REFUNDED", "REFUNDED",
}


@dataclass(frozen=True, slots=True, kw_only=True)
class ShopifyIngestionReport:
    business_id: str
    source_id: str
    mode: str
    extracted_records: int
    projected_events: int
    checkpoint_utc: datetime | None
    persisted: bool
    sample: tuple[dict[str, Any], ...] = ()
    quality_issues: tuple[dict[str, str], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "business_id": self.business_id,
            "source_id": self.source_id,
            "mode": self.mode,
            "extracted_records": self.extracted_records,
            "projected_events": self.projected_events,
            "checkpoint_utc": (self.checkpoint_utc.isoformat().replace("+00:00", "Z")
                               if self.checkpoint_utc else None),
            "persisted": self.persisted,
            "sample": list(self.sample),
            "quality_issues": list(self.quality_issues),
        }


class BronzePersister(Protocol):
    def persist(self, rows: Sequence[dict[str, Any]]) -> None: ...


class SparkBronzePersister:
    """Append prevalidated connector rows to the existing Bronze Parquet path."""

    def __init__(self, path: Path | None = None):
        self.path = path

    def persist(self, rows: Sequence[dict[str, Any]]) -> None:
        if not rows:
            return
        from src.streaming.silver_streaming import BRONZE_VALID_SCHEMA, load_silver_paths
        from src.analytics.gold_build import build_gold_spark_session

        path = self.path or load_silver_paths().bronze_source
        spark = build_gold_spark_session(app_name="pulse-shopify-bronze", master=os.getenv("SPARK_MASTER", "local[*]"))
        spark.sparkContext.setLogLevel("WARN")
        try:
            (spark.createDataFrame(list(rows), BRONZE_VALID_SCHEMA)
             .write.mode("append").partitionBy("ingestion_date").parquet(str(path)))
        finally:
            spark.stop()


def _event_id(config: SourceConfig, order_id: str, suffix: str) -> str:
    return str(uuid5(
        NAMESPACE_URL,
        f"shopify-event|{config.business_id}|{config.source_id}|{order_id}|{suffix}",
    ))


def _as_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def project_order(
    config: SourceConfig,
    business: BusinessConfig,
    envelope: IngestionEnvelope,
    order: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Project a Shopify order snapshot into non-fabricated commerce events."""

    order_id = str(order["order_id"])
    timestamp = _as_datetime(order.get("processed_at_utc") or order["created_at_utc"])
    reporting_date = timestamp.astimezone(ZoneInfo(business.reporting_timezone)).date()
    country = order.get("country") or business.country
    cancelled = bool(order.get("cancelled_at_utc"))
    paid = str(order.get("financial_status", "")).upper() in RECOGNIZED_FINANCIAL_STATUSES
    active = paid and not cancelled
    line_items = list(order.get("line_items") or [])
    gross_weights = [max(float(item.get("line_amount") or 0), 0.0) for item in line_items]
    weight_total = sum(gross_weights)
    recognized_total = max(float(order["total_amount"]) - float(order.get("refunded_amount") or 0), 0.0)
    if line_items and weight_total <= 0:
        gross_weights = [1.0] * len(line_items)
        weight_total = float(len(line_items))

    base = {
        "business_id": config.business_id,
        "source_type": config.source_type,
        "source_id": config.source_id,
        "schema_version": "marketplace_events_v1",
        "event_timestamp": timestamp,
        "customer_id": order.get("customer_id"),
        "session_id": None,
        "country": country,
        "seller_id": None,
        "order_id": order_id,
        "currency": order["currency"],
        "is_active": active,
        "reporting_date": reporting_date,
        "ingestion_id": envelope.ingestion_id,
        "record_id": envelope.record_id,
        "extracted_at_utc": envelope.extracted_at_utc,
        "source_updated_at_utc": envelope.source_updated_at_utc,
        "source_schema_version": envelope.schema_version,
        "kafka_key": None,
        "kafka_topic": None,
        "kafka_partition": None,
        "kafka_offset": None,
        "kafka_timestamp": None,
        "raw_json": json.dumps(envelope.to_dict(), separators=(",", ":"), sort_keys=True),
        "validation_errors": [],
        "ingested_at_utc": envelope.extracted_at_utc,
        "ingestion_date": envelope.extracted_at_utc.date(),
    }
    rows: list[dict[str, Any]] = []
    if not line_items:
        rows.append({
            **base,
            "event_id": _event_id(config, order_id, "payment:order"),
            "event_type": "payment_completed",
            "product_id": None,
            "payment_id": order_id,
            "quantity": None,
            "unit_price": None,
            "event_amount": recognized_total,
            "event_scope": "order",
        })
    else:
        allocated = 0.0
        for index, (item, weight) in enumerate(zip(line_items, gross_weights)):
            amount = (recognized_total - allocated if index == len(line_items) - 1
                      else round(recognized_total * weight / weight_total, 10))
            allocated += amount
            rows.append({
                **base,
                "event_id": _event_id(config, order_id, f"payment:{item['line_item_id']}"),
                "event_type": "payment_completed",
                "product_id": item.get("product_id"),
                "payment_id": order_id,
                "quantity": int(item["quantity"]),
                "unit_price": float(item["unit_price"]),
                "event_amount": max(amount, 0.0),
                "event_scope": "line",
            })
    for refund in order.get("refunds") or []:
        refund_timestamp = _as_datetime(refund.get("created_at_utc") or order["updated_at_utc"])
        rows.append({
            **base,
            "event_id": _event_id(config, order_id, f"refund:{refund['refund_id']}"),
            "event_type": "order_refunded",
            "event_timestamp": refund_timestamp,
            "reporting_date": refund_timestamp.astimezone(ZoneInfo(business.reporting_timezone)).date(),
            "product_id": None,
            "payment_id": str(refund["refund_id"]),
            "quantity": None,
            "unit_price": None,
            "event_amount": float(refund["amount"]),
            "event_scope": "order",
        })
    return rows


def tombstones(
    rows: Sequence[dict[str, Any]], previous_ids: set[str], current_ids: set[str]
) -> list[dict[str, Any]]:
    """Deactivate projections removed by an edited Shopify order."""

    missing = previous_ids - current_ids
    if not missing or not rows:
        return []
    template = rows[0]
    return [{**template, "event_id": event_id, "is_active": False,
             "event_amount": 0.0, "quantity": None, "unit_price": None,
             "product_id": None} for event_id in sorted(missing)]


def resolve_backfill_start(config: SourceConfig, environ: Mapping[str, str]) -> datetime:
    reference = str(config.metadata.get("backfill_start_ref", ""))
    value = environ.get(reference, "").strip()
    if not value:
        raise ShopifyAdapterError(
            f"Initial extraction requires backfill start reference {reference!r}",
            status="configuration_invalid",
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ShopifyAdapterError("Shopify backfill start must be ISO-8601") from None
    if parsed.utcoffset() is None:
        raise ShopifyAdapterError("Shopify backfill start must include a timezone")
    return parsed.astimezone(timezone.utc)


def run_shopify_ingestion(
    config: SourceConfig,
    business: BusinessConfig,
    *,
    adapter: ShopifyAdminApiAdapter | None = None,
    state_store: ConnectorStateStore | None = None,
    persister: BronzePersister | None = None,
    environ: Mapping[str, str] | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    now: Callable[[], datetime] | None = None,
) -> ShopifyIngestionReport:
    environment = os.environ if environ is None else environ
    selected_adapter = adapter or ShopifyAdminApiAdapter(config, environ=environment)
    clock = now or (lambda: datetime.now(timezone.utc))
    store = state_store or ConnectorStateStore(environ=environment)
    state = store.get(config.business_id, config.source_id)
    checkpoint = state.checkpoint_utc
    backfill_start = None if checkpoint else resolve_backfill_start(config, environment)
    mode = "incremental" if checkpoint else "backfill"
    attempted_at = clock().astimezone(timezone.utc)
    if not dry_run:
        store.record_attempt(config.business_id, config.source_id, attempted_at)
    try:
        envelopes = selected_adapter.extract(
            watermark=checkpoint, backfill_start=backfill_start, limit=limit
        )
        rows: list[dict[str, Any]] = []
        projections: list[tuple[str, set[str]]] = []
        normalized_sample = []
        quality_issues = []
        for envelope in envelopes:
            order = selected_adapter.normalize(envelope)
            normalized_sample.append(order)
            from src.quality.models import Severity
            from src.quality.shopify import check_shopify_order

            issues = check_shopify_order(order)
            quality_issues.extend(issues)
            if any(issue.severity == Severity.CRITICAL for issue in issues):
                raise ShopifyAdapterError("Shopify order failed critical data-quality checks")
            projected = project_order(config, business, envelope, order)
            current_ids = {row["event_id"] for row in projected}
            previous_ids = store.projection_ids(
                config.business_id, config.source_id, str(order["order_id"])
            )
            projected.extend(tombstones(projected, previous_ids, current_ids))
            rows.extend(projected)
            projections.append((str(order["order_id"]), current_ids))
        from src.quality.shopify import duplicate_order_versions

        duplicate_issues = duplicate_order_versions(normalized_sample)
        quality_issues.extend(duplicate_issues)
        if duplicate_issues:
            raise ShopifyAdapterError("Shopify batch failed duplicate order/version protection")
        from src.quality.models import Severity
        from src.quality.shopify import duplicate_order_versions

        duplicate_issues = duplicate_order_versions(normalized_sample)
        quality_issues.extend(duplicate_issues)
        if any(issue.severity == Severity.CRITICAL for issue in duplicate_issues):
            raise ShopifyAdapterError("Shopify batch contains duplicate order versions")
        candidate = max(
            (item.source_updated_at_utc for item in envelopes if item.source_updated_at_utc),
            default=checkpoint,
        )
        if not dry_run:
            (persister or SparkBronzePersister()).persist(rows)
            store.record_success(
                config.business_id, config.source_id, clock().astimezone(timezone.utc),
                candidate, len(envelopes), projections,
            )
        return ShopifyIngestionReport(
            business_id=config.business_id,
            source_id=config.source_id,
            mode=mode,
            extracted_records=len(envelopes),
            projected_events=len(rows),
            checkpoint_utc=candidate,
            persisted=not dry_run,
            sample=tuple(normalized_sample[:5]) if dry_run else (),
            quality_issues=tuple(issue.to_dict() for issue in quality_issues),
        )
    except Exception as error:
        if not dry_run:
            error_text = str(error)
            token = environment.get(config.credential_ref)
            if token:
                error_text = error_text.replace(token, "[REDACTED]")
            store.record_failure(
                config.business_id, config.source_id, error_text,
                health=getattr(error, "status", "unhealthy"),
            )
            if error_text != str(error):
                raise ShopifyAdapterError(error_text) from None
        raise
