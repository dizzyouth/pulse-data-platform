"""Tests for marketplace event generation."""

from __future__ import annotations

import json
import subprocess
import sys
import unittest
import uuid
from datetime import datetime, timezone

from src.producers.event_generator import MarketplaceEventGenerator
from src.producers.models import EventType


REFERENCE_TIME = datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc)


class MarketplaceEventGeneratorTests(unittest.TestCase):
    def test_generates_requested_count_with_shared_fields(self) -> None:
        events = list(
            MarketplaceEventGenerator(seed=11, reference_time=REFERENCE_TIME).generate(25)
        )

        self.assertEqual(len(events), 25)
        for event in events:
            self.assertEqual(event.event_timestamp.tzinfo, timezone.utc)
            self.assertIn(event.country, {"US", "GB", "DE", "FR", "CA", "AU", "JP"})
            for identifier, prefix in (
                (event.event_id, "evt_"),
                (event.customer_id, "cus_"),
                (event.session_id, "ses_"),
            ):
                self.assertTrue(identifier.startswith(prefix))
                uuid.UUID(identifier.removeprefix(prefix))

    def test_seed_and_reference_time_make_output_reproducible(self) -> None:
        first = MarketplaceEventGenerator(seed=42)
        second = MarketplaceEventGenerator(seed=42)

        self.assertEqual(
            [event.to_dict() for event in first.generate(10)],
            [event.to_dict() for event in second.generate(10)],
        )

    def test_event_specific_fields(self) -> None:
        generator = MarketplaceEventGenerator(seed=7, reference_time=REFERENCE_TIME)

        viewed = generator.generate_event(EventType.PRODUCT_VIEWED)
        payment = generator.generate_event(EventType.PAYMENT_COMPLETED)

        self.assertIsNotNone(viewed.product_id)
        self.assertIsNone(viewed.quantity)
        self.assertIsNone(viewed.order_id)
        self.assertIsNotNone(payment.order_id)
        self.assertIsNotNone(payment.payment_id)
        self.assertGreaterEqual(payment.quantity or 0, 1)
        self.assertGreater(payment.unit_price or 0, 0)

    def test_serialization_uses_json_friendly_values(self) -> None:
        event = MarketplaceEventGenerator(
            seed=3, reference_time=REFERENCE_TIME
        ).generate_event(EventType.ORDER_CREATED)

        payload = event.to_dict()
        encoded = json.dumps(payload)

        self.assertEqual(payload["event_type"], "order_created")
        self.assertTrue(payload["event_timestamp"].endswith("Z"))
        self.assertNotIn("payment_id", payload)
        self.assertIsInstance(encoded, str)

    def test_negative_count_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            list(MarketplaceEventGenerator(seed=1).generate(-1))

    def test_cli_emits_requested_number_of_json_lines(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "src.producers.event_generator", "--count", "3", "--seed", "9"],
            check=True,
            capture_output=True,
            text=True,
        )

        lines = result.stdout.splitlines()
        self.assertEqual(len(lines), 3)
        for line in lines:
            self.assertIn(json.loads(line)["event_type"], {event.value for event in EventType})


if __name__ == "__main__":
    unittest.main()
