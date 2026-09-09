"""Deterministic record-level quality checks for normalized Shopify orders."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping, Sequence

from src.quality.models import Severity


@dataclass(frozen=True, slots=True, kw_only=True)
class ShopifyQualityIssue:
    code: str
    severity: Severity
    message: str

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "severity": self.severity.value, "message": self.message}


def check_shopify_order(order: Mapping[str, Any]) -> tuple[ShopifyQualityIssue, ...]:
    issues: list[ShopifyQualityIssue] = []

    def add(code: str, severity: Severity, message: str) -> None:
        issues.append(ShopifyQualityIssue(code=code, severity=severity, message=message))

    if not order.get("order_id"):
        add("missing_order_id", Severity.CRITICAL, "order_id is required")
    if not order.get("updated_at_utc"):
        add("missing_updated_at", Severity.CRITICAL, "updated_at_utc is required")
    if not order.get("business_id") or not order.get("source_id"):
        add("missing_source_identity", Severity.CRITICAL, "business_id and source_id are required")
    if not order.get("customer_id"):
        add("guest_customer", Severity.INFO, "guest order is excluded from customer metrics")
    if not re.fullmatch(r"[A-Z]{3}", str(order.get("currency", ""))):
        add("invalid_currency", Severity.CRITICAL, "currency must be a three-letter uppercase code")
    money_fields = (
        "total_amount", "subtotal_amount", "discount_amount", "tax_amount",
        "shipping_amount", "refunded_amount",
    )
    for field in money_fields:
        value = order.get(field)
        if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0):
            add("invalid_monetary_value", Severity.CRITICAL, f"{field} must be nonnegative")
    for item in order.get("line_items") or []:
        if not item.get("line_item_id"):
            add("missing_line_item_id", Severity.CRITICAL, "line item identity is required")
        quantity = item.get("quantity")
        if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity < 0:
            add("invalid_quantity", Severity.CRITICAL, "line-item quantity must be nonnegative")
    required = ("subtotal_amount", "discount_amount", "tax_amount", "shipping_amount")
    if all(order.get(name) is not None for name in required):
        expected = (float(order["subtotal_amount"]) - float(order["discount_amount"])
                    + float(order["tax_amount"]) + float(order["shipping_amount"]))
        if abs(expected - float(order["total_amount"])) > 0.02:
            add(
                "monetary_consistency",
                Severity.WARNING,
                "order components differ from total (duties, tips, edits, or rounding may explain it)",
            )
    return tuple(issues)


def duplicate_order_versions(orders: Sequence[Mapping[str, Any]]) -> tuple[ShopifyQualityIssue, ...]:
    grains = [(item.get("business_id"), item.get("source_id"), item.get("order_id"),
               item.get("updated_at_utc")) for item in orders]
    if len(grains) != len(set(grains)):
        return (ShopifyQualityIssue(
            code="duplicate_order_version",
            severity=Severity.CRITICAL,
            message="The extraction batch contains a duplicate business/source/order/version grain",
        ),)
    return ()
