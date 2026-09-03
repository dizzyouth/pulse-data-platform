"""Schemas for marketplace events."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any


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

    def __post_init__(self) -> None:
        if self.event_timestamp.tzinfo is None:
            raise ValueError("event_timestamp must be timezone-aware")
        if self.event_timestamp.utcoffset() != timezone.utc.utcoffset(None):
            raise ValueError("event_timestamp must use UTC")
        if self.quantity is not None and self.quantity < 1:
            raise ValueError("quantity must be positive")
        if self.unit_price is not None and self.unit_price < 0:
            raise ValueError("unit_price cannot be negative")

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation, omitting unused fields."""

        data = asdict(self)
        data["event_type"] = self.event_type.value
        data["event_timestamp"] = self.event_timestamp.isoformat().replace("+00:00", "Z")
        return {key: value for key, value in data.items() if value is not None}
