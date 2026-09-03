"""Tests for marketplace event generation."""

from __future__ import annotations

import json
import subprocess
import sys
import unittest
import uuid
from datetime import datetime, timezone

from src.producers.event_generator import JourneyProbabilities, MarketplaceEventGenerator
from src.producers.models import EventType


REFERENCE_TIME = datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc)
ALL_STAGES = JourneyProbabilities(
    add_to_cart=1.0,
    start_checkout=1.0,
    create_order=1.0,
    complete_payment=1.0,
    ship_order=1.0,
    deliver_order=1.0,
    refund_order=1.0,
)


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


class CustomerJourneyTests(unittest.TestCase):
    def _complete_journey(self, seed: int = 1):
        return MarketplaceEventGenerator(
            seed=seed,
            reference_time=REFERENCE_TIME,
            journey_probabilities=ALL_STAGES,
        ).generate_journey()

    def test_complete_journey_has_chronological_stage_order(self) -> None:
        journey = self._complete_journey()

        self.assertEqual([event.event_type for event in journey], list(EventType))
        timestamps = [event.event_timestamp for event in journey]
        self.assertEqual(timestamps, sorted(timestamps))
        self.assertTrue(all(first < second for first, second in zip(timestamps, timestamps[1:])))

    def test_journey_keeps_customer_session_and_product_consistent(self) -> None:
        journey = self._complete_journey()

        self.assertEqual(len({event.customer_id for event in journey}), 1)
        self.assertEqual(len({event.session_id for event in journey}), 1)
        self.assertEqual(len({event.product_id for event in journey}), 1)
        self.assertEqual(len({event.seller_id for event in journey}), 1)

    def test_order_events_share_order_id(self) -> None:
        journey = self._complete_journey()
        order_events = journey[3:]

        self.assertEqual(len({event.order_id for event in order_events}), 1)
        self.assertIsNotNone(order_events[0].order_id)

    def test_refund_has_preceding_created_paid_and_delivered_order(self) -> None:
        journey = self._complete_journey()
        types = [event.event_type for event in journey]
        refund_index = types.index(EventType.ORDER_REFUNDED)

        for required in (
            EventType.ORDER_CREATED,
            EventType.PAYMENT_COMPLETED,
            EventType.ORDER_DELIVERED,
        ):
            self.assertLess(types.index(required), refund_index)

    def test_seeded_journeys_are_deterministic(self) -> None:
        first = MarketplaceEventGenerator(seed=42)
        second = MarketplaceEventGenerator(seed=42)

        self.assertEqual(
            [event.to_dict() for event in first.generate_journeys(20)],
            [event.to_dict() for event in second.generate_journeys(20)],
        )

    def test_configured_drop_off_can_produce_view_only_journey(self) -> None:
        view_only = JourneyProbabilities(add_to_cart=0.0)
        journey = MarketplaceEventGenerator(
            seed=5,
            reference_time=REFERENCE_TIME,
            journey_probabilities=view_only,
        ).generate_journey()

        self.assertEqual([event.event_type for event in journey], [EventType.PRODUCT_VIEWED])

    def test_cli_emits_complete_journeys_as_json_lines(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "src.producers.event_generator", "--journeys", "5", "--seed", "42"],
            check=True,
            capture_output=True,
            text=True,
        )
        payloads = [json.loads(line) for line in result.stdout.splitlines()]

        self.assertEqual(
            sum(payload["event_type"] == "product_viewed" for payload in payloads),
            5,
        )


if __name__ == "__main__":
    unittest.main()
