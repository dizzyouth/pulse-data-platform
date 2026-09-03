"""Generate realistic, reproducible Pulse marketplace events."""

from __future__ import annotations

import argparse
import json
import random
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, fields
from datetime import datetime, timedelta, timezone

from .models import EventType, MarketplaceEvent

COUNTRIES_AND_CURRENCIES: tuple[tuple[str, str], ...] = (
    ("US", "USD"),
    ("GB", "GBP"),
    ("DE", "EUR"),
    ("FR", "EUR"),
    ("CA", "CAD"),
    ("AU", "AUD"),
    ("JP", "JPY"),
)

EVENT_TYPES: tuple[EventType, ...] = tuple(EventType)
EVENT_WEIGHTS: tuple[int, ...] = (42, 20, 9, 8, 7, 5, 5, 4)
PRODUCT_EVENTS = {EventType.PRODUCT_VIEWED, EventType.PRODUCT_ADDED_TO_CART}
PAYMENT_EVENTS = {EventType.PAYMENT_COMPLETED, EventType.ORDER_REFUNDED}
SEEDED_REFERENCE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class JourneyProbabilities:
    """Conditional probabilities of progressing to each journey stage."""

    add_to_cart: float = 0.55
    start_checkout: float = 0.65
    create_order: float = 0.72
    complete_payment: float = 0.92
    ship_order: float = 0.97
    deliver_order: float = 0.96
    refund_order: float = 0.08

    def __post_init__(self) -> None:
        for field in fields(self):
            probability = getattr(self, field.name)
            if not 0.0 <= probability <= 1.0:
                raise ValueError(f"{field.name} must be between 0 and 1")


class MarketplaceEventGenerator:
    """Create marketplace events without coupling generation to output transport."""

    def __init__(
        self,
        seed: int | None = None,
        *,
        reference_time: datetime | None = None,
        journey_probabilities: JourneyProbabilities | None = None,
    ) -> None:
        self._random = random.Random(seed)
        self._journey_probabilities = journey_probabilities or JourneyProbabilities()
        self._reference_time = reference_time or (
            SEEDED_REFERENCE_TIME if seed is not None else datetime.now(timezone.utc)
        )
        if self._reference_time.tzinfo is None:
            raise ValueError("reference_time must be timezone-aware")
        self._reference_time = self._reference_time.astimezone(timezone.utc)

    def generate_event(self, event_type: EventType | None = None) -> MarketplaceEvent:
        """Generate one event, choosing a weighted event type when omitted."""

        selected_type = event_type or self._random.choices(
            EVENT_TYPES, weights=EVENT_WEIGHTS, k=1
        )[0]
        country, currency = self._random.choice(COUNTRIES_AND_CURRENCIES)
        timestamp = self._reference_time - timedelta(
            seconds=self._random.randint(0, 86_400)
        )

        common = {
            "event_id": self._uuid("evt"),
            "event_type": selected_type,
            "event_timestamp": timestamp,
            "customer_id": self._uuid("cus"),
            "session_id": self._uuid("ses"),
            "country": country,
        }

        if selected_type in PRODUCT_EVENTS:
            details = self._product_details(currency)
            if selected_type is EventType.PRODUCT_VIEWED:
                details.pop("quantity")
        else:
            details = {"order_id": self._uuid("ord"), **self._product_details(currency)}
            if selected_type in PAYMENT_EVENTS:
                details["payment_id"] = self._uuid("pay")

        return MarketplaceEvent(**common, **details)

    def generate(self, count: int) -> Iterator[MarketplaceEvent]:
        """Yield ``count`` independently generated events."""

        if count < 0:
            raise ValueError("count cannot be negative")
        for _ in range(count):
            yield self.generate_event()

    def generate_journey(self) -> list[MarketplaceEvent]:
        """Generate one chronological customer journey with consistent entities."""

        country, currency = self._random.choice(COUNTRIES_AND_CURRENCIES)
        customer_id = self._uuid("cus")
        session_id = self._uuid("ses")
        product = self._product_details(currency)
        quantity = product.pop("quantity")
        timestamp = self._reference_time
        common = {
            "customer_id": customer_id,
            "session_id": session_id,
            "country": country,
            **product,
        }
        events = [self._journey_event(EventType.PRODUCT_VIEWED, timestamp, common)]

        transitions: tuple[tuple[EventType, float, tuple[int, int]], ...] = (
            (EventType.PRODUCT_ADDED_TO_CART, self._journey_probabilities.add_to_cart, (10, 300)),
            (EventType.CHECKOUT_STARTED, self._journey_probabilities.start_checkout, (15, 600)),
            (EventType.ORDER_CREATED, self._journey_probabilities.create_order, (30, 900)),
            (EventType.PAYMENT_COMPLETED, self._journey_probabilities.complete_payment, (5, 180)),
            (EventType.ORDER_SHIPPED, self._journey_probabilities.ship_order, (3_600, 172_800)),
            (EventType.ORDER_DELIVERED, self._journey_probabilities.deliver_order, (86_400, 604_800)),
            (EventType.ORDER_REFUNDED, self._journey_probabilities.refund_order, (3_600, 1_209_600)),
        )
        order_id: str | None = None
        payment_id: str | None = None

        for event_type, probability, delay_range in transitions:
            if self._random.random() >= probability:
                break
            timestamp += timedelta(seconds=self._random.randint(*delay_range))
            details = {**common, "quantity": quantity}
            if event_type is EventType.ORDER_CREATED:
                order_id = self._uuid("ord")
            if order_id is not None:
                details["order_id"] = order_id
            if event_type is EventType.PAYMENT_COMPLETED:
                payment_id = self._uuid("pay")
            if payment_id is not None and event_type in {
                EventType.PAYMENT_COMPLETED,
                EventType.ORDER_REFUNDED,
            }:
                details["payment_id"] = payment_id
            events.append(self._journey_event(event_type, timestamp, details))

        return events

    def generate_journeys(self, count: int) -> Iterator[MarketplaceEvent]:
        """Yield events from ``count`` journeys in journey order."""

        if count < 0:
            raise ValueError("count cannot be negative")
        for _ in range(count):
            yield from self.generate_journey()

    def _journey_event(
        self,
        event_type: EventType,
        timestamp: datetime,
        details: dict[str, str | int | float],
    ) -> MarketplaceEvent:
        return MarketplaceEvent(
            event_id=self._uuid("evt"),
            event_type=event_type,
            event_timestamp=timestamp,
            **details,
        )

    def _product_details(self, currency: str) -> dict[str, str | int | float]:
        price_ranges = {
            "JPY": (500, 75_000),
            "USD": (5, 750),
            "GBP": (4, 600),
            "EUR": (5, 700),
            "CAD": (7, 950),
            "AUD": (8, 1_100),
        }
        low, high = price_ranges[currency]
        return {
            "product_id": self._uuid("prd"),
            "seller_id": self._uuid("sel"),
            "quantity": self._random.randint(1, 5),
            "unit_price": round(self._random.uniform(low, high), 2),
            "currency": currency,
        }

    def _uuid(self, prefix: str) -> str:
        # Random-derived UUIDs make every generated field repeatable for a seed.
        value = uuid.UUID(int=self._random.getrandbits(128), version=4)
        return f"{prefix}_{value}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate Pulse marketplace events")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--count", type=int, help="number of independent events to emit")
    mode.add_argument("--journeys", type=int, help="number of customer journeys to emit")
    parser.add_argument("--seed", type=int, help="optional seed for reproducible values")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    count = (
        args.journeys
        if args.journeys is not None
        else (args.count if args.count is not None else 1)
    )
    if count < 0:
        raise SystemExit("event and journey counts must be zero or greater")

    generator = MarketplaceEventGenerator(seed=args.seed)
    events = (
        generator.generate_journeys(count)
        if args.journeys is not None
        else generator.generate(count)
    )
    for event in events:
        print(json.dumps(event.to_dict(), separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
