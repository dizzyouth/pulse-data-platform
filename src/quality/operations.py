"""Record-level Phase 6.2 quality checks using the shared severity vocabulary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from src.operations.models import OperationalStatus, STATUS_STAGE, valid_transition
from src.quality.models import Severity


@dataclass(frozen=True, slots=True)
class OperationsQualityIssue:
    code: str
    severity: Severity
    message: str
    business_id: str | None = None
    order_id: str | None = None


def check_operations(silver: Mapping[str, Sequence[Mapping[str, Any]]]) -> tuple[OperationsQualityIssue, ...]:
    """Check data defects; valid negative business outcomes are never failures."""
    issues: list[OperationsQualityIssue] = []
    def add(code, severity, message, row=None):
        row = row or {}
        issues.append(OperationsQualityIssue(code, severity, message,
                                             row.get("business_id"), row.get("order_id")))

    orders = {(r.get("business_id"), r.get("order_id")): r for r in silver.get("commerce_orders", ())}
    events = silver.get("operational_events", ())
    event_ids = set()
    for row in events:
        if not all(row.get(name) not in (None, "") for name in
                   ("business_id", "source_id", "provider", "event_id", "order_id", "canonical_status")):
            add("event_identity_incomplete", Severity.CRITICAL, "Operational event identity is incomplete", row)
        if row.get("event_id") in event_ids:
            add("duplicate_event", Severity.CRITICAL, "Canonical event identity is duplicated", row)
        event_ids.add(row.get("event_id"))
        if (row.get("business_id"), row.get("order_id")) not in orders:
            add("orphan_event", Severity.CRITICAL, "Operational event has no commerce order", row)

    histories: dict[tuple[Any, Any], list[Mapping[str, Any]]] = {}
    for row in events:
        histories.setdefault((row.get("business_id"), row.get("order_id")), []).append(row)
    for history in histories.values():
        history.sort(key=lambda row: (row["event_at"], row.get("revision", 1),
                                      STATUS_STAGE[OperationalStatus(row["canonical_status"])], row["event_id"]))
        for previous, current in zip(history, history[1:]):
            if not valid_transition(previous["canonical_status"], current["canonical_status"],
                                    correction=bool(current.get("corrects_event_id"))):
                add("invalid_status_transition", Severity.WARNING,
                    f"Invalid transition {previous['canonical_status']} -> {current['canonical_status']}", current)
        shipped = [r["event_at"] for r in history if r["canonical_status"] == OperationalStatus.SHIPPED]
        delivered = [r["event_at"] for r in history if r["canonical_status"] == OperationalStatus.DELIVERED]
        if shipped and delivered and min(delivered) < min(shipped):
            add("delivered_before_shipped", Severity.CRITICAL,
                "Delivery occurrence precedes shipment occurrence", history[-1])

    shipment_keys = {(r["business_id"], r["shipment_id"]) for r in silver.get("shipments", ())}
    for row in silver.get("shipments", ()):
        if (row["business_id"], row["order_id"]) not in orders:
            add("orphan_shipment", Severity.CRITICAL, "Shipment has no commerce order", row)
        if row.get("delivered_at") and row.get("shipped_at") and row["delivered_at"] < row["shipped_at"]:
            add("shipment_timestamp_order", Severity.CRITICAL, "delivered_at precedes shipped_at", row)
    for row in silver.get("cash_collections", ()):
        if min(row.get("cash_expected", 0), row.get("cash_collected", 0)) < 0:
            add("negative_collection_amount", Severity.CRITICAL, "Collection amounts cannot be negative", row)
        if row.get("shipment_id") and (row["business_id"], row["shipment_id"]) not in shipment_keys:
            add("orphan_collection_shipment", Severity.CRITICAL, "Collection references an unknown shipment", row)
        compatible = any(e["business_id"] == row["business_id"] and e["order_id"] == row["order_id"]
                         and e["canonical_status"] == OperationalStatus.DELIVERED for e in events)
        if row.get("cash_collected", 0) > 0 and not compatible:
            add("cash_without_delivery", Severity.WARNING,
                "Cash was collected without a compatible delivered event", row)
    remittance_ids = {(r["business_id"], r["remittance_id"]) for r in silver.get("remittances", ())}
    for row in silver.get("cash_collections", ()):
        if row.get("remittance_id") and (row["business_id"], row["remittance_id"]) not in remittance_ids:
            add("orphan_collection_remittance", Severity.WARNING,
                "Collection references a remittance not yet supplied", row)
    for row in silver.get("remittances", ()):
        if not row.get("currency"):
            add("remittance_currency_missing", Severity.CRITICAL, "Remittance currency is required", row)
        if any(row.get(name, 0) < 0 for name in
               ("gross_collected", "provider_fees", "shipping_fees", "cod_fees", "net_remitted")):
            add("negative_remittance_amount", Severity.CRITICAL, "Remittance amounts cannot be negative", row)
        if row.get("net_remitted", 0) > row.get("gross_collected", 0):
            add("net_above_gross", Severity.WARNING,
                "Net remitted exceeds gross collected; inspect adjustments", row)
    return tuple(issues)
