"""Strict, explicitly versioned source payload contracts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any


class SourceContractError(ValueError):
    """A source payload or schema version violates its declared contract."""


@dataclass(frozen=True, slots=True, kw_only=True)
class FieldContract:
    types: tuple[type, ...]
    required: bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class SourceContract:
    source_type: str
    schema_version: str
    fields: dict[str, FieldContract]
    unique_grain: tuple[str, ...]
    source_timestamp: str
    currency_fields: tuple[str, ...] = ()
    timezone: str = "UTC"

    def validate(self, payload: dict[str, Any]):
        if not isinstance(payload, dict):
            raise SourceContractError("Source payload must be an object")
        missing = sorted(name for name, spec in self.fields.items()
                         if spec.required and name not in payload)
        if missing:
            raise SourceContractError("Missing required field(s): " + ", ".join(missing))
        unexpected = sorted(set(payload) - set(self.fields))
        if unexpected:
            raise SourceContractError("Unsupported field(s) for schema version: " + ", ".join(unexpected))
        for name, value in payload.items():
            spec = self.fields[name]
            if value is not None and (
                (isinstance(value, bool) and bool not in spec.types)
                or not isinstance(value, spec.types)
            ):
                expected = "/".join(item.__name__ for item in spec.types)
                raise SourceContractError(f"{name} must be {expected}")
        for name in self.unique_grain:
            if payload.get(name) in (None, ""):
                raise SourceContractError(f"Unique-grain field {name} cannot be empty")
        timestamp = payload.get(self.source_timestamp)
        if timestamp is not None:
            try:
                parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except (TypeError, ValueError):
                raise SourceContractError(f"{self.source_timestamp} must be an ISO-8601 timestamp") from None
            if parsed.utcoffset() is None:
                raise SourceContractError(f"{self.source_timestamp} must include a timezone")
        for name in self.currency_fields:
            currency = payload.get(name)
            if currency is not None and not re.fullmatch(r"[A-Z]{3}", currency):
                raise SourceContractError(f"{name} must be an uppercase ISO-style currency code")


OPTIONAL_STRING = FieldContract(types=(str,), required=False)
OPTIONAL_NUMBER = FieldContract(types=(int, float), required=False)
OPTIONAL_LIST = FieldContract(types=(list,), required=False)

CONTRACTS = {
    ("shopify", "shopify_orders_v1"): SourceContract(
        source_type="shopify", schema_version="shopify_orders_v1",
        fields={
            "order_id": FieldContract(types=(str,)),
            "order_name": OPTIONAL_STRING,
            "customer_id": OPTIONAL_STRING,
            "created_at_utc": FieldContract(types=(str,)),
            "updated_at_utc": FieldContract(types=(str,)),
            "processed_at_utc": OPTIONAL_STRING,
            "cancelled_at_utc": OPTIONAL_STRING,
            "financial_status": FieldContract(types=(str,)),
            "fulfillment_status": FieldContract(types=(str,)),
            "currency": FieldContract(types=(str,)),
            "total_amount": FieldContract(types=(int, float)),
            "subtotal_amount": OPTIONAL_NUMBER,
            "discount_amount": OPTIONAL_NUMBER,
            "tax_amount": OPTIONAL_NUMBER,
            "shipping_amount": OPTIONAL_NUMBER,
            "refunded_amount": OPTIONAL_NUMBER,
            "country": OPTIONAL_STRING,
            "line_items": OPTIONAL_LIST,
            "refunds": OPTIONAL_LIST,
            # Phase 5.9 compatibility fields. Real records use line_items.
            "product_id": OPTIONAL_STRING,
            "quantity": OPTIONAL_NUMBER,
        },
        unique_grain=("order_id",), source_timestamp="updated_at_utc", currency_fields=("currency",)),
    ("meta_ads", "meta_ads_daily_v1"): SourceContract(
        source_type="meta_ads", schema_version="meta_ads_daily_v1",
        fields={"metric_date": FieldContract(types=(str,)), "campaign_id": FieldContract(types=(str,)),
                "account_id": FieldContract(types=(str,)), "currency": FieldContract(types=(str,)),
                "spend": FieldContract(types=(int, float)), "impressions": FieldContract(types=(int,)),
                "clicks": FieldContract(types=(int,)), "updated_at_utc": FieldContract(types=(str,))},
        unique_grain=("metric_date", "campaign_id"), source_timestamp="updated_at_utc",
        currency_fields=("currency",)),
    ("csv_manual", "csv_business_events_v1"): SourceContract(
        source_type="csv_manual", schema_version="csv_business_events_v1",
        fields={"record_id": FieldContract(types=(str,)), "event_type": FieldContract(types=(str,)),
                "event_timestamp": FieldContract(types=(str,)), "value": OPTIONAL_NUMBER,
                "currency": OPTIONAL_STRING, "customer_id": OPTIONAL_STRING,
                "product_id": OPTIONAL_STRING},
        unique_grain=("record_id",), source_timestamp="event_timestamp", currency_fields=("currency",)),
}

SUPPORTED_SOURCE_TYPES = frozenset({"shopify", "meta_ads", "tiktok_ads", "google_ads", "csv_manual"})


def contract_for(source_type: str, schema_version: str):
    try:
        return CONTRACTS[(source_type, schema_version)]
    except KeyError:
        raise SourceContractError(
            f"Unsupported schema version {schema_version!r} for source type {source_type!r}"
        ) from None
