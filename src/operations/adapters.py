"""Offline operational adapters layered on the existing SourceAdapter contract."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from src.onboarding.adapters import AdapterHealth, MockSourceAdapter
from src.onboarding.contracts import contract_for
from src.onboarding.models import IngestionEnvelope, SourceConfig
from src.operations.models import ConfirmationOutcome, OperationalStatus, PaymentType, SettlementStatus


PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = PROJECT_ROOT / "data" / "fixtures" / "operations"
EXTRACTED_AT = datetime(2026, 2, 1, 12, tzinfo=timezone.utc)
DEFAULT_LOOKBACK_DAYS = 7

OPERATIONS_SOURCE_TYPES = frozenset({
    "commerce_orders", "confirmation_events", "fulfillment_events",
    "delivery_events", "cod_collections", "remittances",
})

STATUS_MAPPINGS: dict[str, dict[str, OperationalStatus]] = {
    "commerce_orders": {
        "new": OperationalStatus.CREATED,
        "awaiting_confirmation": OperationalStatus.PENDING_CONFIRMATION,
        "cancelled_by_merchant": OperationalStatus.CANCELLED,
    },
    "confirmation_events": {
        "queued": OperationalStatus.PENDING_CONFIRMATION,
        "customer_confirmed": OperationalStatus.CONFIRMED,
        "customer_rejected": OperationalStatus.REJECTED_CONFIRMATION,
        "customer_not_answering": OperationalStatus.UNREACHABLE,
        "duplicate_order": OperationalStatus.REJECTED_CONFIRMATION,
        "fraud_flag": OperationalStatus.REJECTED_CONFIRMATION,
        "invalid": OperationalStatus.REJECTED_CONFIRMATION,
        "cancelled": OperationalStatus.CANCELLED,
    },
    "fulfillment_events": {
        "ready": OperationalStatus.READY_FOR_FULFILLMENT,
        "packed": OperationalStatus.FULFILLED,
        "handed_to_carrier": OperationalStatus.SHIPPED,
    },
    "delivery_events": {
        "moving": OperationalStatus.IN_TRANSIT,
        "vehicle_out": OperationalStatus.OUT_FOR_DELIVERY,
        "attempted": OperationalStatus.DELIVERY_ATTEMPTED,
        "customer_not_answering": OperationalStatus.UNREACHABLE,
        "customer_refused": OperationalStatus.REFUSED,
        "new_date_requested": OperationalStatus.RESCHEDULED,
        "parcel_delivered": OperationalStatus.DELIVERED,
        "return_started": OperationalStatus.RETURN_IN_TRANSIT,
        "returned_to_sender": OperationalStatus.RETURNED_TO_ORIGIN,
    },
    "cod_collections": {
        "cash_received": OperationalStatus.CASH_COLLECTED,
        "awaiting_settlement": OperationalStatus.REMITTANCE_PENDING,
    },
    "remittances": {
        "pending": OperationalStatus.REMITTANCE_PENDING,
        "paid": OperationalStatus.REMITTED,
        "partial": OperationalStatus.REMITTANCE_PENDING,
        "cancelled": OperationalStatus.REMITTANCE_PENDING,
    },
}

CONFIRMATION_OUTCOMES = {
    "queued": ConfirmationOutcome.PENDING,
    "customer_confirmed": ConfirmationOutcome.CONFIRMED,
    "customer_rejected": ConfirmationOutcome.REJECTED,
    "customer_not_answering": ConfirmationOutcome.CUSTOMER_UNREACHABLE,
    "duplicate_order": ConfirmationOutcome.DUPLICATE,
    "fraud_flag": ConfirmationOutcome.FRAUD_SUSPECTED,
    "cancelled": ConfirmationOutcome.CANCELLED,
    "invalid": ConfirmationOutcome.INVALID_ORDER,
}


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() is None:
        raise ValueError("operational timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def lookback_start(watermark: datetime | None, lookback_days: int = DEFAULT_LOOKBACK_DAYS):
    if not isinstance(lookback_days, int) or isinstance(lookback_days, bool) or lookback_days < 0:
        raise ValueError("lookback_days must be a nonnegative integer")
    return None if watermark is None else _utc(watermark.isoformat()) - timedelta(days=lookback_days)


class CommerceOperationsSourceAdapter(MockSourceAdapter):
    """Shared deterministic extractor for independent operational roles."""

    def __init__(self, config: SourceConfig):
        super().__init__(config)
        self.lookback_days = config.metadata.get("lookback_days", DEFAULT_LOOKBACK_DAYS)

    def validate_config(self) -> tuple[str, ...]:
        errors = list(super().validate_config())
        if self.config.source_type not in OPERATIONS_SOURCE_TYPES:
            errors.append("source is not a commerce-operations role")
        if self.config.metadata.get("adapter", "mock") != "mock":
            errors.append("operations adapters are offline mock-only in Phase 6.2")
        fixture = str(self.config.metadata.get("fixture", ""))
        if not fixture or Path(fixture).name != fixture or not (FIXTURE_ROOT / fixture).is_file():
            errors.append("metadata.fixture must name an operations fixture")
        try:
            lookback_start(EXTRACTED_AT, self.lookback_days)
        except ValueError as error:
            errors.append(str(error))
        return tuple(errors)

    def raw_payloads(self) -> tuple[dict[str, Any], ...]:
        value = json.loads((FIXTURE_ROOT / self.config.metadata["fixture"]).read_text(encoding="utf-8"))
        if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
            raise ValueError("Operations fixture must contain an object array")
        return tuple(value)

    def extract(self, *, watermark: datetime | None = None) -> tuple[IngestionEnvelope, ...]:
        errors = self.validate_config()
        if errors:
            raise ValueError("; ".join(errors))
        start = lookback_start(watermark, self.lookback_days)
        contract = contract_for(self.config.source_type, self.config.schema_version)
        ingestion_id = str(uuid5(NAMESPACE_URL,
            f"operations-ingestion|{self.config.business_id}|{self.config.source_id}|{start or 'full'}"))
        output = []
        for payload in self.raw_payloads():
            contract.validate(payload)
            if start is not None and _utc(payload[contract.source_timestamp]) < start:
                continue
            revision = int(payload.get("revision") or 1)
            identity = "|".join((self.config.business_id, self.config.source_id,
                                 payload["provider"], payload["external_event_id"], str(revision)))
            output.append(IngestionEnvelope(
                business_id=self.config.business_id, source_type=self.config.source_type,
                source_id=self.config.source_id, ingestion_id=ingestion_id,
                record_id=str(uuid5(NAMESPACE_URL, f"operations-record|{identity}")),
                extracted_at_utc=EXTRACTED_AT,
                source_updated_at_utc=_utc(payload[contract.source_timestamp]),
                schema_version=self.config.schema_version, payload=dict(payload),
            ))
        return tuple(output)

    def _event(self, record: IngestionEnvelope) -> dict[str, Any]:
        p = record.payload
        status = STATUS_MAPPINGS[record.source_type].get(p["provider_status"])
        if status is None:
            raise ValueError(f"Unmapped provider status {p['provider_status']!r}")
        revision = int(p.get("revision") or 1)
        event_id = str(uuid5(NAMESPACE_URL, "|".join((
            "operations-event", record.business_id, record.source_id, p["provider"],
            p["external_event_id"], str(revision),
        ))))
        return {
            "business_id": record.business_id, "source_type": record.source_type,
            "source_id": record.source_id, "provider": p["provider"],
            "event_id": event_id, "external_event_id": p["external_event_id"],
            "order_id": p["order_id"], "shipment_id": p.get("shipment_id"),
            "event_type": record.source_type.removesuffix("_events").removesuffix("s"),
            "canonical_status": status.value, "provider_status": p["provider_status"],
            "event_at": _utc(p["event_at"]), "received_at_utc": _utc(p["received_at_utc"]),
            "revision": revision, "corrects_event_id": p.get("corrects_event_id"),
            "details": dict(p.get("details") or {}),
        }

    def normalize(self, record: IngestionEnvelope) -> dict[str, Any]:
        if (record.business_id, record.source_type, record.source_id) != (
                self.config.business_id, self.config.source_type, self.config.source_id):
            raise ValueError("Envelope identity does not match adapter configuration")
        contract_for(record.source_type, record.schema_version).validate(record.payload)
        p = record.payload
        base = {"record_id": record.record_id, "ingestion_id": record.ingestion_id,
                "schema_version": record.schema_version,
                "event": None if record.source_type == "remittances" else self._event(record)}
        if record.source_type == "commerce_orders":
            return {**base, "entity_type": "commerce_order", "order": {
                "business_id": record.business_id, "source_type": record.source_type,
                "source_id": record.source_id, "provider": p["provider"],
                "order_id": p["order_id"], "external_order_id": p.get("external_order_id"),
                "order_created_at": _utc(p["order_created_at"]),
                "order_updated_at": _utc(p["order_updated_at"]),
                "source_timezone": p["source_timezone"], "currency": p["currency"],
                "order_value": float(p["order_value"]), "payment_type": PaymentType(p["payment_type"]).value,
                "customer_reference": p.get("customer_reference"), "lines": tuple(p["lines"]),
            }}
        if record.source_type == "confirmation_events":
            return {**base, "entity_type": "confirmation", "confirmation": {
                "outcome": CONFIRMATION_OUTCOMES[p["provider_status"]].value,
                "provider_reason_code": p.get("provider_reason_code"),
            }}
        if record.source_type == "fulfillment_events":
            return {**base, "entity_type": "shipment", "shipment": {
                "business_id": record.business_id, "order_id": p["order_id"],
                "fulfillment_id": p["fulfillment_id"], "shipment_id": p.get("shipment_id"),
                "provider": p["provider"], "courier": p.get("courier"),
                "tracking_reference": p.get("tracking_reference"),
                "shipment_created_at": _utc(p["shipment_created_at"]) if p.get("shipment_created_at") else None,
                "shipped_at": _utc(p["shipped_at"]) if p.get("shipped_at") else None,
                "delivered_at": _utc(p["delivered_at"]) if p.get("delivered_at") else None,
                "return_at": _utc(p["return_at"]) if p.get("return_at") else None,
                "line_quantities": dict(p.get("line_quantities") or {}),
            }}
        if record.source_type == "delivery_events":
            return {**base, "entity_type": "delivery", "delivery_attempt": (
                {"attempt_number": p["attempt_number"], "attempt_outcome": p["attempt_outcome"]}
                if p.get("attempt_number") is not None else None),
                "courier": p["courier"], "tracking_reference": p.get("tracking_reference")}
        if record.source_type == "cod_collections":
            return {**base, "entity_type": "cash_collection", "collection": {
                "business_id": record.business_id, "source_id": record.source_id,
                "provider": p["provider"], "collection_id": p["collection_id"],
                "order_id": p["order_id"], "shipment_id": p.get("shipment_id"),
                "cash_expected": float(p["cash_expected"]), "cash_collected": float(p["cash_collected"]),
                "collection_currency": p["collection_currency"],
                "collected_at": _utc(p["collected_at"]) if p.get("collected_at") else None,
                "remittance_id": p.get("remittance_id"),
            }}
        return {**base, "entity_type": "remittance", "event": None, "remittance": {
            "business_id": record.business_id, "source_id": record.source_id,
            "provider": p["provider"], "remittance_id": p["remittance_id"],
            "currency": p["currency"], "period_start": p["period_start"], "period_end": p["period_end"],
            "gross_collected": float(p["gross_collected"]), "provider_fees": float(p["provider_fees"]),
            "shipping_fees": float(p["shipping_fees"]), "cod_fees": float(p["cod_fees"]),
            "adjustments": float(p["adjustments"]), "net_remitted": float(p["net_remitted"]),
            "remitted_at": _utc(p["remitted_at"]) if p.get("remitted_at") else None,
            "settlement_status": SettlementStatus({"paid": "remitted"}.get(p["provider_status"], p["provider_status"])).value,
            "order_links": tuple(p["order_links"]), "event_at": _utc(p["event_at"]),
            "received_at_utc": _utc(p["received_at_utc"]), "external_event_id": p["external_event_id"],
            "provider_status": p["provider_status"], "revision": int(p.get("revision") or 1),
        }}

    def healthcheck(self) -> AdapterHealth:
        errors = self.validate_config()
        return AdapterHealth(healthy=not errors, message="; ".join(errors) if errors
                             else "offline commerce-operations fixture; no network access")


def operations_adapter_for(config: SourceConfig) -> CommerceOperationsSourceAdapter:
    if config.source_type not in OPERATIONS_SOURCE_TYPES:
        raise ValueError(f"Source type {config.source_type!r} is not an operations source")
    return CommerceOperationsSourceAdapter(config)
