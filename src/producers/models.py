"""Schemas for marketplace events."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from src.onboarding import (
    DEFAULT_BUSINESS_ID,
    DEFAULT_MARKETPLACE_SCHEMA_VERSION,
    DEFAULT_SOURCE_ID,
    DEFAULT_SOURCE_TYPE,
)
from src.onboarding.models import IDENTIFIER_PATTERN


class EventType(str, Enum):
    """Event types emitted by the marketplace generator."""

    PRODUCT_VIEWED = "product_viewed"
    PRODUCT_ADDED_TO_CART = "product_added_to_cart"
    CHECKOUT_STARTED = "checkout_started"
    ORDER_CREATED = "order_created"
    PAYMENT_COMPLETED = "payment_completed"
    ORDER_SHIPPED = "order_shipped"
    ORDER_DELIVERED = "order_delivered"
    ORDER_REFUNDED = "order_refunded"


@dataclass(frozen=True, slots=True)
class MarketplaceEvent:
    """Shared schema for events produced by the Pulse marketplace."""

    event_id: str
    event_type: EventType
    event_timestamp: datetime
    customer_id: str
    session_id: str
    country: str
    product_id: str | None = None
    seller_id: str | None = None
    order_id: str | None = None
    payment_id: str | None = None
    quantity: int | None = None
    unit_price: float | None = None
    currency: str | None = None
    business_id: str = DEFAULT_BUSINESS_ID
    source_type: str = DEFAULT_SOURCE_TYPE
    source_id: str = DEFAULT_SOURCE_ID
    schema_version: str = DEFAULT_MARKETPLACE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.event_timestamp.tzinfo is None:
            raise ValueError("event_timestamp must be timezone-aware")
        if self.event_timestamp.utcoffset() != timezone.utc.utcoffset(None):
            raise ValueError("event_timestamp must use UTC")
        if self.quantity is not None and self.quantity < 1:
            raise ValueError("quantity must be positive")
        if self.unit_price is not None and self.unit_price < 0:
            raise ValueError("unit_price cannot be negative")
        for field_name in ("business_id", "source_type", "source_id"):
            if not IDENTIFIER_PATTERN.fullmatch(getattr(self, field_name)):
                raise ValueError(f"{field_name} must be a stable lowercase identifier")
        if not self.schema_version.strip():
            raise ValueError("schema_version cannot be empty")

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation, omitting unused fields."""

        data = asdict(self)
        data["event_type"] = self.event_type.value
        data["event_timestamp"] = self.event_timestamp.isoformat().replace("+00:00", "Z")
        return {key: value for key, value in data.items() if value is not None}
