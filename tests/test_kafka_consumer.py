"""Unit tests for the marketplace Kafka consumer adapter."""

from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from src.streaming.kafka_consumer import (
    MarketplaceKafkaConsumer,
    MarketplaceMessageError,
    deserialize_marketplace_message,
)


VALID_PAYLOAD = {
    "event_id": "evt_1",
    "event_type": "product_viewed",
    "event_timestamp": "2026-01-01T00:00:00Z",
    "customer_id": "cus_1",
    "session_id": "ses_1",
    "country": "US",
}


class FakeMessage:
    def __init__(self, value: bytes, key: bytes = b"cus_1", error=None) -> None:
        self._value = value
        self._key = key
        self._error = error

    def value(self):
        return self._value

    def key(self):
        return self._key

    def error(self):
        return self._error


def valid_message(**updates) -> FakeMessage:
    payload = {**VALID_PAYLOAD, **updates}
    return FakeMessage(json.dumps(payload).encode("utf-8"))


class FakeConsumer:
    def __init__(self, messages) -> None:
        self.messages = list(messages)
        self.subscriptions: list[list[str]] = []
        self.commits = []
        self.closed = False
        self.poll_count = 0

    def subscribe(self, topics: list[str]) -> None:
        self.subscriptions.append(topics)

    def poll(self, timeout: float):
        self.poll_count += 1
        return self.messages.pop(0) if self.messages else None

    def commit(self, *, message, asynchronous: bool):
        self.commits.append((message, asynchronous))

    def close(self) -> None:
        self.closed = True


class MarketplaceMessageTests(unittest.TestCase):
    def test_deserializes_json_and_validates_key(self) -> None:
        self.assertEqual(deserialize_marketplace_message(valid_message()), VALID_PAYLOAD)

    def test_rejects_malformed_json(self) -> None:
        with self.assertRaisesRegex(MarketplaceMessageError, "valid UTF-8 JSON"):
            deserialize_marketplace_message(FakeMessage(b"{not-json"))

    def test_rejects_missing_required_fields(self) -> None:
        payload = {key: value for key, value in VALID_PAYLOAD.items() if key != "event_id"}
        with self.assertRaisesRegex(MarketplaceMessageError, "event_id"):
            deserialize_marketplace_message(
                FakeMessage(json.dumps(payload).encode("utf-8"))
            )

    def test_rejects_customer_key_mismatch(self) -> None:
        with self.assertRaisesRegex(MarketplaceMessageError, "does not match"):
            deserialize_marketplace_message(valid_message(customer_id="cus_other"))


class MarketplaceKafkaConsumerTests(unittest.TestCase):
    def make_consumer(self, fake: FakeConsumer) -> MarketplaceKafkaConsumer:
        return MarketplaceKafkaConsumer(
            "localhost:9092",
            "marketplace.events",
            "test-group",
            consumer=fake,
        )

    def test_builds_required_confluent_configuration(self) -> None:
        with patch("src.streaming.kafka_consumer.Consumer") as consumer_class:
            MarketplaceKafkaConsumer(
                "broker:9092", "marketplace.events", "pulse-group", "latest"
            )

        config = consumer_class.call_args.args[0]
        self.assertEqual(config["bootstrap.servers"], "broker:9092")
        self.assertEqual(config["group.id"], "pulse-group")
        self.assertEqual(config["auto.offset.reset"], "latest")
        self.assertIs(config["enable.auto.commit"], False)

    def test_subscribes_processes_commits_and_closes(self) -> None:
        message = valid_message()
        fake = FakeConsumer([message])
        processed = []

        result = self.make_consumer(fake).consume(
            processed.append, max_messages=1, idle_timeout=0
        )

        self.assertEqual(fake.subscriptions, [["marketplace.events"]])
        self.assertEqual(processed, [VALID_PAYLOAD])
        self.assertEqual(fake.commits, [(message, False)])
        self.assertTrue(fake.closed)
        self.assertEqual((result.consumed, result.processed, result.failed), (1, 1, 0))

    def test_failed_message_is_not_committed(self) -> None:
        bad_message = FakeMessage(b"not-json")
        fake = FakeConsumer([bad_message])
        errors = []

        result = self.make_consumer(fake).consume(
            lambda payload: None,
            max_messages=1,
            idle_timeout=0,
            on_error=errors.append,
        )

        self.assertEqual(fake.commits, [])
        self.assertEqual(result.failed, 1)
        self.assertIn("valid UTF-8 JSON", errors[0])
        self.assertTrue(fake.closed)

    def test_processing_failure_is_not_committed(self) -> None:
        fake = FakeConsumer([valid_message()])

        result = self.make_consumer(fake).consume(
            lambda payload: (_ for _ in ()).throw(RuntimeError("processing failed")),
            max_messages=1,
            idle_timeout=0,
        )

        self.assertEqual(fake.commits, [])
        self.assertEqual(result.failed, 1)
        self.assertTrue(fake.closed)

    def test_max_messages_bounds_consumption(self) -> None:
        fake = FakeConsumer([valid_message(), valid_message(), valid_message()])

        result = self.make_consumer(fake).consume(
            lambda payload: None, max_messages=2, idle_timeout=0
        )

        self.assertEqual(result.consumed, 2)
        self.assertEqual(fake.poll_count, 2)
        self.assertEqual(len(fake.commits), 2)
        self.assertTrue(fake.closed)

    def test_keyboard_interrupt_closes_consumer(self) -> None:
        class InterruptingConsumer(FakeConsumer):
            def poll(self, timeout: float):
                raise KeyboardInterrupt

        fake = InterruptingConsumer([])
        result = self.make_consumer(fake).consume(lambda payload: None)

        self.assertTrue(result.interrupted)
        self.assertTrue(fake.closed)


if __name__ == "__main__":
    unittest.main()
