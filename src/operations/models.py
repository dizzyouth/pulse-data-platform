"""Canonical Phase 6.2 operational entities, lifecycle, and KPI semantics."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from enum import StrEnum
import math
import re
from typing import Any, Iterable

from src.marketing.models import safe_divide
from src.onboarding.models import IDENTIFIER_PATTERN


class OperationalStatus(StrEnum):
    CREATED = "CREATED"
    PENDING_CONFIRMATION = "PENDING_CONFIRMATION"
    CONFIRMED = "CONFIRMED"
    REJECTED_CONFIRMATION = "REJECTED_CONFIRMATION"
    CANCELLED = "CANCELLED"
    READY_FOR_FULFILLMENT = "READY_FOR_FULFILLMENT"
    FULFILLED = "FULFILLED"
    SHIPPED = "SHIPPED"
    IN_TRANSIT = "IN_TRANSIT"
    OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY"
    DELIVERY_ATTEMPTED = "DELIVERY_ATTEMPTED"
    DELIVERED = "DELIVERED"
    REFUSED = "REFUSED"
    UNREACHABLE = "UNREACHABLE"
    RESCHEDULED = "RESCHEDULED"
    RETURN_IN_TRANSIT = "RETURN_IN_TRANSIT"
    RETURNED_TO_ORIGIN = "RETURNED_TO_ORIGIN"
    CASH_COLLECTED = "CASH_COLLECTED"
    REMITTANCE_PENDING = "REMITTANCE_PENDING"
    REMITTED = "REMITTED"


class PaymentType(StrEnum):
    COD = "cod"
    PREPAID = "prepaid"
    MIXED = "mixed"
    OTHER = "other"


class ConfirmationOutcome(StrEnum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    CUSTOMER_UNREACHABLE = "customer_unreachable"
    DUPLICATE = "duplicate"
    FRAUD_SUSPECTED = "fraud_suspected"
    CANCELLED = "cancelled"
    INVALID_ORDER = "invalid_order"


class SettlementStatus(StrEnum):
    PENDING = "pending"
    REMITTED = "remitted"
    PARTIAL = "partial"
    CANCELLED = "cancelled"


STATUS_STAGE = {
    status: index for index, statuses in enumerate((
        (OperationalStatus.CREATED,),
        (OperationalStatus.PENDING_CONFIRMATION,),
        (OperationalStatus.CONFIRMED, OperationalStatus.REJECTED_CONFIRMATION,
         OperationalStatus.CANCELLED, OperationalStatus.UNREACHABLE),
        (OperationalStatus.READY_FOR_FULFILLMENT,),
        (OperationalStatus.FULFILLED,),
        (OperationalStatus.SHIPPED,),
        (OperationalStatus.IN_TRANSIT,),
        (OperationalStatus.OUT_FOR_DELIVERY,),
        (OperationalStatus.DELIVERY_ATTEMPTED, OperationalStatus.REFUSED,
         OperationalStatus.RESCHEDULED),
        (OperationalStatus.DELIVERED, OperationalStatus.RETURN_IN_TRANSIT),
        (OperationalStatus.RETURNED_TO_ORIGIN, OperationalStatus.CASH_COLLECTED),
        (OperationalStatus.REMITTANCE_PENDING,),
        (OperationalStatus.REMITTED,),
    )) for status in statuses
}

TERMINAL_BEFORE_FULFILLMENT = {
    OperationalStatus.REJECTED_CONFIRMATION, OperationalStatus.CANCELLED,
}


def valid_transition(previous: OperationalStatus | str, current: OperationalStatus | str,
                     *, correction: bool = False) -> bool:
    """Validate lifecycle movement while allowing explicit provider corrections.

    Lifecycles may skip optional stages. Delivery retries and late delivery after
    refusal/unreachable are valid. A correction is retained but explicitly
    marked, so it may revise a provider's earlier truth.
    """
    before, after = OperationalStatus(previous), OperationalStatus(current)
    if correction or before == after:
        return True
    if before in TERMINAL_BEFORE_FULFILLMENT:
        return False
    if before == OperationalStatus.DELIVERED:
        return after in {OperationalStatus.RETURN_IN_TRANSIT,
                         OperationalStatus.RETURNED_TO_ORIGIN,
                         OperationalStatus.CASH_COLLECTED,
                         OperationalStatus.REMITTANCE_PENDING,
                         OperationalStatus.REMITTED}
    if before == OperationalStatus.REFUSED:
        return after in {OperationalStatus.RETURN_IN_TRANSIT,
                         OperationalStatus.RETURNED_TO_ORIGIN,
                         OperationalStatus.DELIVERY_ATTEMPTED,
                         OperationalStatus.RESCHEDULED,
                         OperationalStatus.DELIVERED}
    if after == OperationalStatus.UNREACHABLE and STATUS_STAGE[before] >= STATUS_STAGE[OperationalStatus.SHIPPED]:
        return True
    if before in {OperationalStatus.UNREACHABLE, OperationalStatus.RESCHEDULED,
                  OperationalStatus.DELIVERY_ATTEMPTED}:
        return after in {OperationalStatus.DELIVERY_ATTEMPTED,
                         OperationalStatus.RESCHEDULED, OperationalStatus.IN_TRANSIT,
                         OperationalStatus.REFUSED,
                         OperationalStatus.OUT_FOR_DELIVERY, OperationalStatus.DELIVERED,
                         OperationalStatus.RETURN_IN_TRANSIT,
                         OperationalStatus.RETURNED_TO_ORIGIN}
    return STATUS_STAGE[after] >= STATUS_STAGE[before]


def _utc(value: datetime, name: str) -> datetime:
    if value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _money(value: float, name: str, *, allow_negative: bool = False) -> float:
    number = float(value)
    if not math.isfinite(number) or (number < 0 and not allow_negative):
        raise ValueError(f"{name} must be finite" + ("" if allow_negative else " and nonnegative"))
    return number


def _currency(value: str) -> str:
    if not re.fullmatch(r"[A-Z]{3}", value):
        raise ValueError("currency must be an uppercase ISO-style code")
    return value


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderLine:
    business_id: str
    order_id: str
    line_id: str
    product_id: str | None
    variant_id: str | None
    sku: str | None
    quantity: int
    unit_price: float
    currency: str

    def __post_init__(self):
        if not self.order_id or not self.line_id or self.quantity <= 0:
            raise ValueError("order_id, line_id, and positive quantity are required")
        object.__setattr__(self, "unit_price", _money(self.unit_price, "unit_price"))
        _currency(self.currency)


@dataclass(frozen=True, slots=True, kw_only=True)
class CommerceOrder:
    business_id: str
    source_type: str
    source_id: str
    provider: str
    order_id: str
    external_order_id: str | None
    order_created_at: datetime
    order_updated_at: datetime
    source_timezone: str
    reporting_timezone: str
    currency: str
    order_value: float
    payment_type: PaymentType
    customer_reference: str | None = None
    lines: tuple[OrderLine, ...] = ()

    def __post_init__(self):
        if not IDENTIFIER_PATTERN.fullmatch(self.business_id) or not self.order_id:
            raise ValueError("stable business_id and order_id are required")
        object.__setattr__(self, "order_created_at", _utc(self.order_created_at, "order_created_at"))
        object.__setattr__(self, "order_updated_at", _utc(self.order_updated_at, "order_updated_at"))
        if self.order_updated_at < self.order_created_at:
            raise ValueError("order_updated_at cannot precede order_created_at")
        object.__setattr__(self, "order_value", _money(self.order_value, "order_value"))
        _currency(self.currency)


@dataclass(frozen=True, slots=True, kw_only=True)
class OperationalEvent:
    business_id: str
    source_type: str
    source_id: str
    provider: str
    event_id: str
    external_event_id: str
    order_id: str
    event_type: str
    canonical_status: OperationalStatus
    provider_status: str
    event_at: datetime
    received_at_utc: datetime
    revision: int = 1
    corrects_event_id: str | None = None
    shipment_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        for name in ("provider", "event_id", "external_event_id", "order_id", "provider_status"):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} is required")
        if self.revision < 1:
            raise ValueError("revision must be positive")
        object.__setattr__(self, "event_at", _utc(self.event_at, "event_at"))
        object.__setattr__(self, "received_at_utc", _utc(self.received_at_utc, "received_at_utc"))

    @property
    def logical_identity(self) -> tuple[str, ...]:
        return (self.business_id, self.source_id, self.provider, self.external_event_id)

    @property
    def ordering_key(self) -> tuple[Any, ...]:
        return (self.event_at, self.revision, STATUS_STAGE[self.canonical_status], self.event_id)


@dataclass(frozen=True, slots=True, kw_only=True)
class ConfirmationEvent:
    event: OperationalEvent
    outcome: ConfirmationOutcome
    provider_reason_code: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class Fulfillment:
    business_id: str
    order_id: str
    fulfillment_id: str
    provider: str
    line_quantities: dict[str, int] = field(default_factory=dict)

    def __post_init__(self):
        if not all((self.business_id, self.order_id, self.fulfillment_id, self.provider)):
            raise ValueError("fulfillment identity is required")
        if any(not line_id or isinstance(quantity, bool) or not isinstance(quantity, int) or quantity <= 0
               for line_id, quantity in self.line_quantities.items()):
            raise ValueError("line quantities must be positive integers")


@dataclass(frozen=True, slots=True, kw_only=True)
class Shipment:
    business_id: str
    order_id: str
    fulfillment_id: str
    shipment_id: str
    provider: str
    courier: str | None
    tracking_reference: str | None
    shipment_created_at: datetime | None = None
    shipped_at: datetime | None = None
    delivered_at: datetime | None = None
    return_at: datetime | None = None
    line_quantities: dict[str, int] = field(default_factory=dict)

    def __post_init__(self):
        if not all((self.business_id, self.order_id, self.fulfillment_id, self.shipment_id, self.provider)):
            raise ValueError("shipment identity is required")
        for name in ("shipment_created_at", "shipped_at", "delivered_at", "return_at"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _utc(value, name))
        if self.shipped_at and self.delivered_at and self.delivered_at < self.shipped_at:
            raise ValueError("delivered_at cannot precede shipped_at")
        Fulfillment(business_id=self.business_id, order_id=self.order_id,
                    fulfillment_id=self.fulfillment_id, provider=self.provider,
                    line_quantities=self.line_quantities)


@dataclass(frozen=True, slots=True, kw_only=True)
class DeliveryAttempt:
    event: OperationalEvent
    attempt_number: int
    attempt_outcome: str

    def __post_init__(self):
        if self.attempt_number < 1:
            raise ValueError("attempt_number must be positive")


@dataclass(frozen=True, slots=True, kw_only=True)
class CashCollection:
    business_id: str
    source_id: str
    provider: str
    collection_id: str
    order_id: str
    shipment_id: str | None
    cash_expected: float
    cash_collected: float
    collection_currency: str
    collected_at: datetime | None
    remittance_id: str | None = None

    def __post_init__(self):
        object.__setattr__(self, "cash_expected", _money(self.cash_expected, "cash_expected"))
        object.__setattr__(self, "cash_collected", _money(self.cash_collected, "cash_collected"))
        _currency(self.collection_currency)
        if self.collected_at is not None:
            object.__setattr__(self, "collected_at", _utc(self.collected_at, "collected_at"))


@dataclass(frozen=True, slots=True, kw_only=True)
class Remittance:
    business_id: str
    source_id: str
    provider: str
    remittance_id: str
    currency: str
    period_start: date
    period_end: date
    gross_collected: float
    provider_fees: float
    shipping_fees: float
    cod_fees: float
    adjustments: float
    net_remitted: float
    remitted_at: datetime | None
    settlement_status: SettlementStatus
    order_links: tuple[dict[str, str | None], ...] = ()

    def __post_init__(self):
        if self.period_end < self.period_start:
            raise ValueError("period_end cannot precede period_start")
        _currency(self.currency)
        for name in ("gross_collected", "provider_fees", "shipping_fees", "cod_fees", "net_remitted"):
            object.__setattr__(self, name, _money(getattr(self, name), name))
        object.__setattr__(self, "adjustments", _money(self.adjustments, "adjustments", allow_negative=True))
        if self.remitted_at is not None:
            object.__setattr__(self, "remitted_at", _utc(self.remitted_at, "remitted_at"))


def event_latest_revisions(events: Iterable[OperationalEvent]) -> tuple[OperationalEvent, ...]:
    """Deduplicate retries and keep the highest revision for each native event."""
    latest: dict[tuple[str, ...], OperationalEvent] = {}
    for event in events:
        key = event.logical_identity
        existing = latest.get(key)
        if existing is None or (event.revision, event.received_at_utc, event.event_id) > (
                existing.revision, existing.received_at_utc, existing.event_id):
            latest[key] = event
    return tuple(sorted(latest.values(), key=lambda event: (*event.logical_identity, event.ordering_key)))


def latest_order_events(events: Iterable[OperationalEvent]) -> dict[tuple[str, str], OperationalEvent]:
    latest: dict[tuple[str, str], OperationalEvent] = {}
    for event in event_latest_revisions(events):
        key = (event.business_id, event.order_id)
        if key not in latest or event.ordering_key > latest[key].ordering_key:
            latest[key] = event
    return latest


def operational_kpis(*, eligible_orders: int, confirmed_orders: int, shipped_orders: int,
                     delivered_orders: int, refused_orders: int, returned_orders: int,
                     unreachable_orders: int, delivery_attempts: int,
                     cash_expected: float, cash_collected: float) -> dict[str, float | None]:
    """Explicit Phase 6.2 denominators; never uses marketing conversions."""
    return {
        "confirmation_rate": safe_divide(confirmed_orders, eligible_orders),
        "ship_rate": safe_divide(shipped_orders, confirmed_orders),
        "delivery_rate": safe_divide(delivered_orders, shipped_orders),
        "refusal_rate": safe_divide(refused_orders, shipped_orders),
        "return_rate": safe_divide(returned_orders, delivered_orders),
        "unreachable_rate": safe_divide(unreachable_orders, eligible_orders),
        "average_delivery_attempts": safe_divide(delivery_attempts, shipped_orders),
        "cash_collection_rate": safe_divide(cash_collected, cash_expected),
    }


def to_primitive(value: Any) -> dict[str, Any]:
    output = asdict(value)
    for name, item in tuple(output.items()):
        if isinstance(item, (date, datetime)):
            output[name] = item.isoformat().replace("+00:00", "Z")
        elif isinstance(item, StrEnum):
            output[name] = item.value
    return output
