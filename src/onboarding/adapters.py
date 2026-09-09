"""Future connector interface with deterministic local-only implementations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from src.onboarding.contracts import contract_for
from src.onboarding.models import IngestionEnvelope, SourceConfig


REFERENCE_TIME = datetime(2026, 1, 5, 12, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True, kw_only=True)
class AdapterHealth:
    healthy: bool
    message: str


class SourceAdapter(Protocol):
    config: SourceConfig
    def validate_config(self) -> tuple[str, ...]: ...
    def extract(self) -> tuple[IngestionEnvelope, ...]: ...
    def normalize(self, record: IngestionEnvelope) -> dict: ...
    def healthcheck(self) -> AdapterHealth: ...


class MockSourceAdapter:
    payloads: tuple[dict, ...] = ()

    def __init__(self, config: SourceConfig):
        self.config = config

    def validate_config(self):
        errors = []
        if not self.config.enabled:
            errors.append("source is disabled")
        if not self.config.credential_ref:
            errors.append("credential_ref is required")
        contract_for(self.config.source_type, self.config.schema_version)
        return tuple(errors)

    def extract(self):
        if self.validate_config():
            raise ValueError("Mock adapter configuration is not enabled and complete")
        ingestion_id = str(uuid5(NAMESPACE_URL, f"ingestion|{self.config.business_id}|{self.config.source_id}"))
        output = []
        contract = contract_for(self.config.source_type, self.config.schema_version)
        for payload in self.payloads:
            contract.validate(payload)
            record_key = "|".join(str(payload[field]) for field in contract.unique_grain)
            output.append(IngestionEnvelope(
                business_id=self.config.business_id, source_type=self.config.source_type,
                source_id=self.config.source_id, ingestion_id=ingestion_id,
                record_id=str(uuid5(NAMESPACE_URL, f"{ingestion_id}|{record_key}")),
                extracted_at_utc=REFERENCE_TIME, source_updated_at_utc=REFERENCE_TIME,
                schema_version=self.config.schema_version, payload=dict(payload)))
        return tuple(output)

    def normalize(self, record):
        if (record.business_id, record.source_type, record.source_id) != (
                self.config.business_id, self.config.source_type, self.config.source_id):
            raise ValueError("Envelope identity does not match adapter configuration")
        contract_for(record.source_type, record.schema_version).validate(record.payload)
        return {"business_id": record.business_id, "source_type": record.source_type,
                "source_id": record.source_id, "record_id": record.record_id,
                "schema_version": record.schema_version, **record.payload}

    def healthcheck(self):
        return AdapterHealth(healthy=not self.validate_config(), message="local mock; no network access")


class MockShopifyAdapter(MockSourceAdapter):
    payloads = (
        {"order_id": "ord_shared", "customer_id": "cus_shared", "created_at_utc": "2026-01-04T10:00:00Z",
         "updated_at_utc": "2026-01-04T10:05:00Z", "currency": "USD", "total_amount": 120.0,
         "financial_status": "PAID", "fulfillment_status": "FULFILLED",
         "product_id": "prd_shared", "quantity": 2},
        {"order_id": "ord_demo_2", "customer_id": "cus_demo_2", "created_at_utc": "2026-01-05T10:00:00Z",
         "updated_at_utc": "2026-01-05T10:05:00Z", "currency": "USD", "total_amount": 80.0,
         "financial_status": "PAID", "fulfillment_status": "UNFULFILLED",
         "product_id": "prd_demo_2", "quantity": 1},
    )


class MockMetaAdsAdapter(MockSourceAdapter):
    payloads = (
        {"metric_date": "2026-01-04", "campaign_id": "cmp_shared", "account_id": "act_demo",
         "currency": "USD", "spend": 25.0, "impressions": 1000, "clicks": 40,
         "updated_at_utc": "2026-01-05T08:00:00Z"},
        {"metric_date": "2026-01-05", "campaign_id": "cmp_demo_2", "account_id": "act_demo",
         "currency": "USD", "spend": 30.0, "impressions": 1200, "clicks": 45,
         "updated_at_utc": "2026-01-05T09:00:00Z"},
    )


class MockCsvAdapter(MockSourceAdapter):
    payloads = (
        {"record_id": "csv_1", "event_type": "store_opened",
         "event_timestamp": "2026-01-04T09:00:00Z", "value": 1},
        {"record_id": "csv_2", "event_type": "inventory_snapshot",
         "event_timestamp": "2026-01-05T09:00:00Z", "value": 42, "product_id": "prd_shared"},
    )


ADAPTERS = {"shopify": MockShopifyAdapter, "meta_ads": MockMetaAdsAdapter,
            "csv_manual": MockCsvAdapter}


def adapter_for(config: SourceConfig):
    if config.source_type == "shopify" and config.metadata.get("adapter") == "admin_api":
        from src.onboarding.shopify import ShopifyAdminApiAdapter

        return ShopifyAdminApiAdapter(config)
    if config.source_type == "shopify" and config.metadata.get("adapter", "mock") != "mock":
        raise ValueError("Unsupported Shopify adapter selection")
    try:
        return ADAPTERS[config.source_type](config)
    except KeyError:
        raise ValueError(f"No local adapter for source type {config.source_type!r}") from None
