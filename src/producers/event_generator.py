"""Generate realistic, reproducible Pulse marketplace events."""

from __future__ import annotations

import argparse
import json
import random
import uuid
from collections.abc import Iterator, Sequence
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


class MarketplaceEventGenerator:
    """Create marketplace events without coupling generation to output transport."""

    def __init__(
        self,
        seed: int | None = None,
        *,
        reference_time: datetime | None = None,
    ) -> None:
        self._random = random.Random(seed)
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
    parser.add_argument("--count", type=int, default=1, help="number of events to emit")
    parser.add_argument("--seed", type=int, help="optional seed for reproducible values")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.count < 0:
        raise SystemExit("--count must be zero or greater")

    generator = MarketplaceEventGenerator(seed=args.seed)
    for event in generator.generate(args.count):
        print(json.dumps(event.to_dict(), separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
