"""Unit tests for the marketplace Kafka transport adapter."""

from __future__ import annotations

import json
import unittest
from typing import Any

from src.producers.event_generator import MarketplaceEventGenerator
from src.producers.kafka_producer import (
    KafkaPublishingError,
    MarketplaceKafkaPublisher,
)


class FakeProducer:
    def __init__(
        self,
        *,
        delivery_error: Exception | None = None,
        produce_error: Exception | None = None,
        outstanding: int = 0,
    ) -> None:
        self.delivery_error = delivery_error
        self.produce_error = produce_error
        self.outstanding = outstanding
        self.records: list[dict[str, Any]] = []
        self.callbacks: list[Any] = []
        self.poll_calls: list[float] = []
        self.flush_calls: list[float | None] = []

    def produce(self, topic: str, **kwargs: Any) -> None:
        if self.produce_error is not None:
            raise self.produce_error
        self.records.append({"topic": topic, **kwargs})
        self.callbacks.append(kwargs["on_delivery"])

    def poll(self, timeout: float) -> int:
        self.poll_calls.append(timeout)
        return 0

    def flush(self, timeout: float | None = None) -> int:
        self.flush_calls.append(timeout)
        for callback in self.callbacks:
            callback(self.delivery_error, None)
        self.callbacks.clear()
        return self.outstanding


class MarketplaceKafkaPublisherTests(unittest.TestCase):
    def test_publishes_json_to_topic_with_customer_key(self) -> None:
        fake = FakeProducer()
        event = MarketplaceEventGenerator(seed=42).generate_journey()[0]
        publisher = MarketplaceKafkaPublisher(
            "localhost:9092", "marketplace.events", producer=fake
        )

        result = publisher.publish([event])

        self.assertEqual(result.attempted, 1)
        self.assertEqual(result.delivered, 1)
        self.assertEqual(fake.records[0]["topic"], "marketplace.events")
        self.assertEqual(fake.records[0]["key"], event.customer_id.encode("utf-8"))
        self.assertEqual(
            json.loads(fake.records[0]["value"].decode("utf-8")), event.to_dict()
        )

    def test_delivery_failure_is_surfaced_after_flush(self) -> None:
        fake = FakeProducer(delivery_error=RuntimeError("broker rejected record"))
        event = MarketplaceEventGenerator(seed=7).generate_journey()[0]
        publisher = MarketplaceKafkaPublisher(
            "localhost:9092", "marketplace.events", producer=fake
        )

        with self.assertRaisesRegex(KafkaPublishingError, "broker rejected record"):
            publisher.publish([event])

        self.assertEqual(fake.flush_calls, [30.0])

    def test_synchronous_failure_still_flushes(self) -> None:
        fake = FakeProducer(produce_error=RuntimeError("producer unavailable"))
        event = MarketplaceEventGenerator(seed=8).generate_journey()[0]
        publisher = MarketplaceKafkaPublisher(
            "localhost:9092", "marketplace.events", producer=fake
        )

        with self.assertRaisesRegex(KafkaPublishingError, "producer unavailable"):
            publisher.publish([event])

        self.assertEqual(fake.flush_calls, [30.0])

    def test_outstanding_messages_after_flush_are_surfaced(self) -> None:
        fake = FakeProducer(outstanding=1)
        event = MarketplaceEventGenerator(seed=9).generate_journey()[0]
        publisher = MarketplaceKafkaPublisher(
            "localhost:9092", "marketplace.events", producer=fake
        )

        with self.assertRaisesRegex(KafkaPublishingError, "1 message.*still queued"):
            publisher.publish([event])

    def test_seeded_journeys_remain_deterministic_when_published(self) -> None:
        first_fake = FakeProducer()
        second_fake = FakeProducer()
        first = MarketplaceKafkaPublisher("localhost:9092", "events", producer=first_fake)
        second = MarketplaceKafkaPublisher("localhost:9092", "events", producer=second_fake)

        first.publish(MarketplaceEventGenerator(seed=42).generate_journeys(5))
        second.publish(MarketplaceEventGenerator(seed=42).generate_journeys(5))

        first_records = [(record["key"], record["value"]) for record in first_fake.records]
        second_records = [(record["key"], record["value"]) for record in second_fake.records]
        self.assertEqual(first_records, second_records)
        self.assertGreater(len(first_records), 5)


if __name__ == "__main__":
    unittest.main()
