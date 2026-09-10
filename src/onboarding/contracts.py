"""Strict, explicitly versioned source payload contracts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import re
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


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
    timezone_field: str | None = None
    date_fields: tuple[str, ...] = ()
    grain_name: str = "source_record"
    metric_semantics: dict[str, str] | None = None

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
        for name in self.date_fields:
            value = payload.get(name)
            if value is not None:
                try:
                    date.fromisoformat(value)
                except (TypeError, ValueError):
                    raise SourceContractError(f"{name} must be an ISO-8601 date") from None
        if self.timezone_field and payload.get(self.timezone_field) is not None:
            try:
                ZoneInfo(payload[self.timezone_field])
            except (ZoneInfoNotFoundError, ValueError, TypeError):
                raise SourceContractError(
                    f"{self.timezone_field} must be an IANA timezone"
                ) from None


OPTIONAL_STRING = FieldContract(types=(str,), required=False)
OPTIONAL_NUMBER = FieldContract(types=(int, float), required=False)
OPTIONAL_LIST = FieldContract(types=(list,), required=False)
OPTIONAL_INTEGER = FieldContract(types=(int,), required=False)
OPTIONAL_OBJECT = FieldContract(types=(dict,), required=False)

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
        fields={
            "date_start": FieldContract(types=(str,)),
            "account_id": FieldContract(types=(str,)),
            "account_name": OPTIONAL_STRING,
            "campaign_id": FieldContract(types=(str,)), "campaign_name": OPTIONAL_STRING,
            "adset_id": FieldContract(types=(str,)), "adset_name": OPTIONAL_STRING,
            "ad_id": FieldContract(types=(str,)), "ad_name": OPTIONAL_STRING,
            "creative_id": OPTIONAL_STRING,
            "account_timezone": FieldContract(types=(str,)),
            "account_currency": FieldContract(types=(str,)),
            "spend": FieldContract(types=(int, float)),
            "impressions": FieldContract(types=(int,)), "reach": OPTIONAL_INTEGER,
            "frequency": OPTIONAL_NUMBER, "clicks": FieldContract(types=(int,)),
            "inline_link_clicks": OPTIONAL_INTEGER, "actions": OPTIONAL_INTEGER,
            "action_values": OPTIONAL_NUMBER, "video_views": OPTIONAL_INTEGER,
            "landing_page_views": OPTIONAL_INTEGER, "updated_at_utc": FieldContract(types=(str,)),
            "details": OPTIONAL_OBJECT,
        },
        unique_grain=("date_start", "account_id", "campaign_id", "adset_id", "ad_id"),
        source_timestamp="updated_at_utc", currency_fields=("account_currency",),
        timezone_field="account_timezone", date_fields=("date_start",), grain_name="ad_daily",
        metric_semantics={"spend": "additive", "impressions": "additive", "clicks": "additive",
                          "actions": "platform_attributed_additive", "action_values": "platform_attributed_additive",
                          "reach": "non_additive", "frequency": "non_additive"}),
    ("tiktok_ads", "tiktok_ads_daily_v1"): SourceContract(
        source_type="tiktok_ads", schema_version="tiktok_ads_daily_v1",
        fields={
            "stat_time_day": FieldContract(types=(str,)),
            "advertiser_id": FieldContract(types=(str,)),
            "campaign_id": FieldContract(types=(str,)), "campaign_name": OPTIONAL_STRING,
            "adgroup_id": FieldContract(types=(str,)), "adgroup_name": OPTIONAL_STRING,
            "ad_id": FieldContract(types=(str,)), "ad_name": OPTIONAL_STRING,
            "creative_id": OPTIONAL_STRING, "timezone": FieldContract(types=(str,)),
            "currency": FieldContract(types=(str,)), "stat_cost": FieldContract(types=(int, float)),
            "show_cnt": FieldContract(types=(int,)), "reach": OPTIONAL_INTEGER,
            "click_cnt": FieldContract(types=(int,)), "conversion": OPTIONAL_NUMBER,
            "conversion_value": OPTIONAL_NUMBER, "video_play_actions": OPTIONAL_INTEGER,
            "landing_page_view": OPTIONAL_INTEGER, "updated_at_utc": FieldContract(types=(str,)),
            "details": OPTIONAL_OBJECT,
        },
        unique_grain=("stat_time_day", "advertiser_id", "campaign_id", "adgroup_id", "ad_id"),
        source_timestamp="updated_at_utc", currency_fields=("currency",),
        timezone_field="timezone", date_fields=("stat_time_day",), grain_name="ad_daily",
        metric_semantics={"stat_cost": "additive", "show_cnt": "additive", "click_cnt": "additive",
                          "conversion": "platform_attributed_additive", "conversion_value": "platform_attributed_additive",
                          "reach": "non_additive"}),
    ("google_ads", "google_ads_daily_v1"): SourceContract(
        source_type="google_ads", schema_version="google_ads_daily_v1",
        fields={
            "segments_date": FieldContract(types=(str,)),
            "customer_id": FieldContract(types=(str,)),
            "campaign_id": FieldContract(types=(str,)), "campaign_name": OPTIONAL_STRING,
            "ad_group_id": FieldContract(types=(str,)), "ad_group_name": OPTIONAL_STRING,
            "ad_id": FieldContract(types=(str,)), "ad_name": OPTIONAL_STRING,
            "creative_id": OPTIONAL_STRING, "timezone": FieldContract(types=(str,)),
            "currency": FieldContract(types=(str,)), "cost_micros": FieldContract(types=(int,)),
            "impressions": FieldContract(types=(int,)), "clicks": FieldContract(types=(int,)),
            "conversions": OPTIONAL_NUMBER, "conversions_value": OPTIONAL_NUMBER,
            "video_views": OPTIONAL_INTEGER, "updated_at_utc": FieldContract(types=(str,)),
            "details": OPTIONAL_OBJECT,
        },
        unique_grain=("segments_date", "customer_id", "campaign_id", "ad_group_id", "ad_id"),
        source_timestamp="updated_at_utc", currency_fields=("currency",),
        timezone_field="timezone", date_fields=("segments_date",), grain_name="ad_daily",
        metric_semantics={"cost_micros": "additive_currency_micros", "impressions": "additive",
                          "clicks": "additive", "conversions": "platform_attributed_additive",
                          "conversions_value": "platform_attributed_additive"}),
    ("generic_ads", "generic_ads_daily_v1"): SourceContract(
        source_type="generic_ads", schema_version="generic_ads_daily_v1",
        fields={
            "report_date": FieldContract(types=(str,)), "platform": FieldContract(types=(str,)),
            "account_id": FieldContract(types=(str,)),
            "campaign_id": FieldContract(types=(str,)), "campaign_name": OPTIONAL_STRING,
            "ad_group_id": FieldContract(types=(str,)), "ad_group_name": OPTIONAL_STRING,
            "ad_id": FieldContract(types=(str,)), "ad_name": OPTIONAL_STRING,
            "creative_id": OPTIONAL_STRING, "reporting_timezone": FieldContract(types=(str,)),
            "currency": FieldContract(types=(str,)), "spend": FieldContract(types=(int, float)),
            "impressions": FieldContract(types=(int,)), "reach": OPTIONAL_INTEGER,
            "frequency": OPTIONAL_NUMBER, "clicks": FieldContract(types=(int,)),
            "link_clicks": OPTIONAL_INTEGER, "conversions": OPTIONAL_NUMBER,
            "conversion_value": OPTIONAL_NUMBER, "video_views": OPTIONAL_INTEGER,
            "landing_page_views": OPTIONAL_INTEGER, "updated_at_utc": FieldContract(types=(str,)),
            "details": OPTIONAL_OBJECT,
        },
        unique_grain=("report_date", "platform", "account_id", "campaign_id", "ad_group_id", "ad_id"),
        source_timestamp="updated_at_utc", currency_fields=("currency",),
        timezone_field="reporting_timezone", date_fields=("report_date",), grain_name="ad_daily",
        metric_semantics={"spend": "additive", "impressions": "additive", "clicks": "additive",
                          "conversions": "platform_attributed_additive", "conversion_value": "platform_attributed_additive",
                          "reach": "non_additive", "frequency": "non_additive"}),
    ("csv_manual", "csv_business_events_v1"): SourceContract(
        source_type="csv_manual", schema_version="csv_business_events_v1",
        fields={"record_id": FieldContract(types=(str,)), "event_type": FieldContract(types=(str,)),
                "event_timestamp": FieldContract(types=(str,)), "value": OPTIONAL_NUMBER,
                "currency": OPTIONAL_STRING, "customer_id": OPTIONAL_STRING,
                "product_id": OPTIONAL_STRING},
        unique_grain=("record_id",), source_timestamp="event_timestamp", currency_fields=("currency",)),
}

# Phase 6.2 provider-neutral commerce-operations contracts.  Provider-native
# payloads remain in Bronze; these contracts only describe the stable boundary
# each adapter must accept before canonical normalization.
_OPERATION_EVENT_FIELDS = {
    "external_event_id": FieldContract(types=(str,)),
    "order_id": FieldContract(types=(str,)),
    "provider": FieldContract(types=(str,)),
    "provider_status": FieldContract(types=(str,)),
    "event_at": FieldContract(types=(str,)),
    "received_at_utc": FieldContract(types=(str,)),
    "revision": OPTIONAL_INTEGER,
    "corrects_event_id": OPTIONAL_STRING,
    "shipment_id": OPTIONAL_STRING,
    "details": OPTIONAL_OBJECT,
}

CONTRACTS.update({
    ("commerce_orders", "commerce_orders_v1"): SourceContract(
        source_type="commerce_orders", schema_version="commerce_orders_v1",
        fields={
            **_OPERATION_EVENT_FIELDS,
            "external_order_id": OPTIONAL_STRING,
            "order_created_at": FieldContract(types=(str,)),
            "order_updated_at": FieldContract(types=(str,)),
            "source_timezone": FieldContract(types=(str,)),
            "currency": FieldContract(types=(str,)),
            "order_value": FieldContract(types=(int, float)),
            "payment_type": FieldContract(types=(str,)),
            "customer_reference": OPTIONAL_STRING,
            "lines": FieldContract(types=(list,)),
        },
        unique_grain=("external_event_id",), source_timestamp="received_at_utc",
        currency_fields=("currency",), timezone_field="source_timezone",
        grain_name="commerce_order_event",
        metric_semantics={"order_value": "non_additive_order_snapshot"},
    ),
    ("confirmation_events", "confirmation_events_v1"): SourceContract(
        source_type="confirmation_events", schema_version="confirmation_events_v1",
        fields={**_OPERATION_EVENT_FIELDS, "outcome": FieldContract(types=(str,)),
                "provider_reason_code": OPTIONAL_STRING},
        unique_grain=("external_event_id",), source_timestamp="received_at_utc",
        grain_name="confirmation_event",
    ),
    ("fulfillment_events", "fulfillment_events_v1"): SourceContract(
        source_type="fulfillment_events", schema_version="fulfillment_events_v1",
        fields={**_OPERATION_EVENT_FIELDS, "fulfillment_id": FieldContract(types=(str,)),
                "courier": OPTIONAL_STRING, "tracking_reference": OPTIONAL_STRING,
                "shipment_created_at": OPTIONAL_STRING, "shipped_at": OPTIONAL_STRING,
                "delivered_at": OPTIONAL_STRING, "return_at": OPTIONAL_STRING,
                "line_quantities": OPTIONAL_OBJECT},
        unique_grain=("external_event_id",), source_timestamp="received_at_utc",
        grain_name="fulfillment_event",
    ),
    ("delivery_events", "delivery_events_v1"): SourceContract(
        source_type="delivery_events", schema_version="delivery_events_v1",
        fields={**_OPERATION_EVENT_FIELDS, "courier": FieldContract(types=(str,)),
                "tracking_reference": OPTIONAL_STRING, "attempt_number": OPTIONAL_INTEGER,
                "attempt_outcome": OPTIONAL_STRING},
        unique_grain=("external_event_id",), source_timestamp="received_at_utc",
        grain_name="delivery_event",
    ),
    ("cod_collections", "cod_collections_v1"): SourceContract(
        source_type="cod_collections", schema_version="cod_collections_v1",
        fields={**_OPERATION_EVENT_FIELDS, "collection_id": FieldContract(types=(str,)),
                "cash_expected": FieldContract(types=(int, float)),
                "cash_collected": FieldContract(types=(int, float)),
                "collection_currency": FieldContract(types=(str,)),
                "collected_at": OPTIONAL_STRING, "remittance_id": OPTIONAL_STRING},
        unique_grain=("external_event_id",), source_timestamp="received_at_utc",
        currency_fields=("collection_currency",), grain_name="cash_collection_event",
        metric_semantics={"cash_expected": "additive", "cash_collected": "additive"},
    ),
    ("remittances", "remittances_v1"): SourceContract(
        source_type="remittances", schema_version="remittances_v1",
        fields={
            "external_event_id": FieldContract(types=(str,)),
            "remittance_id": FieldContract(types=(str,)),
            "provider": FieldContract(types=(str,)),
            "provider_status": FieldContract(types=(str,)),
            "period_start": FieldContract(types=(str,)), "period_end": FieldContract(types=(str,)),
            "currency": FieldContract(types=(str,)),
            "gross_collected": FieldContract(types=(int, float)),
            "provider_fees": FieldContract(types=(int, float)),
            "shipping_fees": FieldContract(types=(int, float)),
            "cod_fees": FieldContract(types=(int, float)),
            "adjustments": FieldContract(types=(int, float)),
            "net_remitted": FieldContract(types=(int, float)),
            "remitted_at": OPTIONAL_STRING,
            "event_at": FieldContract(types=(str,)),
            "received_at_utc": FieldContract(types=(str,)),
            "revision": OPTIONAL_INTEGER, "corrects_event_id": OPTIONAL_STRING,
            "order_links": FieldContract(types=(list,)), "details": OPTIONAL_OBJECT,
        },
        unique_grain=("external_event_id",), source_timestamp="received_at_utc",
        currency_fields=("currency",), date_fields=("period_start", "period_end"),
        grain_name="remittance_event",
        metric_semantics={"gross_collected": "additive", "net_remitted": "additive"},
    ),
})

SUPPORTED_SOURCE_TYPES = frozenset(
    {"shopify", "meta_ads", "tiktok_ads", "google_ads", "generic_ads", "csv_manual",
     "commerce_orders", "confirmation_events", "fulfillment_events", "delivery_events",
     "cod_collections", "remittances"}
)


def contract_for(source_type: str, schema_version: str):
    try:
        return CONTRACTS[(source_type, schema_version)]
    except KeyError:
        raise SourceContractError(
            f"Unsupported schema version {schema_version!r} for source type {source_type!r}"
        ) from None
