"""Consume and validate Pulse marketplace events from Kafka."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from confluent_kafka import Consumer, KafkaException

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_MARKETPLACE_TOPIC = "marketplace.events"
DEFAULT_CONSUMER_GROUP = "pulse.marketplace.consumer"
DEFAULT_AUTO_OFFSET_RESET = "earliest"

REQUIRED_EVENT_FIELDS = frozenset(
    {
        "event_id",
        "event_type",
        "event_timestamp",
        "customer_id",
        "session_id",
        "country",
    }
)


class ConsumerClient(Protocol):
    def subscribe(self, topics: list[str]) -> None: ...

    def poll(self, timeout: float) -> Any: ...

    def commit(self, *, message: Any, asynchronous: bool) -> Any: ...

    def close(self) -> None: ...


class MarketplaceMessageError(ValueError):
    """Raised when a Kafka record is not a valid marketplace event."""


@dataclass(frozen=True, slots=True)
class ConsumeResult:
    consumed: int
    processed: int
    failed: int
    topic: str
    consumer_group: str
    interrupted: bool = False


def deserialize_marketplace_message(message: Any) -> dict[str, Any]:
    """Decode and validate a marketplace Kafka record."""

    raw_value = message.value()
    if raw_value is None:
        raise MarketplaceMessageError("message value is null")
    try:
        payload = json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise MarketplaceMessageError(f"message value is not valid UTF-8 JSON: {error}") from error
    if not isinstance(payload, dict):
        raise MarketplaceMessageError("message JSON must be an object")

    missing = sorted(REQUIRED_EVENT_FIELDS.difference(payload))
    if missing:
        raise MarketplaceMessageError(
            f"message is missing required field(s): {', '.join(missing)}"
        )

    raw_key = message.key()
    if raw_key is None:
        raise MarketplaceMessageError("Kafka message key is missing")
    try:
        customer_id = raw_key.decode("utf-8")
    except UnicodeDecodeError as error:
        raise MarketplaceMessageError("Kafka message key is not valid UTF-8") from error
    if customer_id != payload["customer_id"]:
        raise MarketplaceMessageError(
            "Kafka message key does not match payload customer_id"
        )
    return payload


class MarketplaceKafkaConsumer:
    """Kafka transport adapter with processing-before-commit semantics.

    Automatic commits are disabled. Each offset is committed synchronously only
    after validation and processing succeed, providing at-least-once delivery.
    A crash after processing but before the commit may result in redelivery.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        consumer_group: str,
        auto_offset_reset: str = DEFAULT_AUTO_OFFSET_RESET,
        *,
        consumer: ConsumerClient | None = None,
    ) -> None:
        if not bootstrap_servers.strip():
            raise ValueError("bootstrap_servers cannot be empty")
        if not topic.strip():
            raise ValueError("topic cannot be empty")
        if not consumer_group.strip():
            raise ValueError("consumer_group cannot be empty")
        if auto_offset_reset not in {"earliest", "latest", "error"}:
            raise ValueError("auto_offset_reset must be earliest, latest, or error")

        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer_group = consumer_group
        self.auto_offset_reset = auto_offset_reset
        self._consumer: ConsumerClient = consumer or Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": consumer_group,
                "auto.offset.reset": auto_offset_reset,
                "enable.auto.commit": False,
                "client.id": "pulse-marketplace-consumer",
            }
        )

    def consume(
        self,
        process: Callable[[Mapping[str, Any]], None],
        *,
        max_messages: int | None = 10,
        poll_timeout: float = 1.0,
        idle_timeout: float | None = 10.0,
        on_error: Callable[[str], None] | None = None,
    ) -> ConsumeResult:
        """Consume until the record limit, idle timeout, or interruption."""

        if max_messages is not None and max_messages < 0:
            raise ValueError("max_messages cannot be negative")
        if idle_timeout is not None and idle_timeout < 0:
            raise ValueError("idle_timeout cannot be negative")

        consumed = processed = failed = 0
        interrupted = False
        last_message_at = time.monotonic()

        try:
            self._consumer.subscribe([self.topic])
            while max_messages is None or consumed < max_messages:
                message = self._consumer.poll(poll_timeout)
                if message is None:
                    if (
                        idle_timeout is not None
                        and time.monotonic() - last_message_at >= idle_timeout
                    ):
                        break
                    continue

                consumed += 1
                last_message_at = time.monotonic()
                try:
                    kafka_error = message.error()
                    if kafka_error is not None:
                        raise KafkaException(kafka_error)
                    payload = deserialize_marketplace_message(message)
                    process(payload)
                    self._consumer.commit(message=message, asynchronous=False)
                    processed += 1
                except Exception as error:
                    failed += 1
                    if on_error is not None:
                        on_error(str(error))
        except KeyboardInterrupt:
            interrupted = True
        finally:
            self._consumer.close()

        return ConsumeResult(
            consumed=consumed,
            processed=processed,
            failed=failed,
            topic=self.topic,
            consumer_group=self.consumer_group,
            interrupted=interrupted,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Consume and validate Pulse marketplace events from Kafka"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--max-messages", type=int, default=10, help="maximum records to consume"
    )
    mode.add_argument(
        "--continuous", action="store_true", help="consume until interrupted"
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=10.0,
        help="exit after this many seconds without a record",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.max_messages is not None and args.max_messages < 0:
        raise SystemExit("max-messages must be zero or greater")
    if args.idle_timeout < 0:
        raise SystemExit("idle-timeout must be zero or greater")

    consumer = MarketplaceKafkaConsumer(
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
        os.getenv("KAFKA_MARKETPLACE_TOPIC", DEFAULT_MARKETPLACE_TOPIC),
        os.getenv("KAFKA_CONSUMER_GROUP", DEFAULT_CONSUMER_GROUP),
        os.getenv("KAFKA_AUTO_OFFSET_RESET", DEFAULT_AUTO_OFFSET_RESET),
    )

    def process(payload: Mapping[str, Any]) -> None:
        print(
            f"{payload['event_type']} customer={payload['customer_id']} "
            f"event={payload['event_id']}"
        )

    result = consumer.consume(
        process,
        max_messages=None if args.continuous else args.max_messages,
        idle_timeout=None if args.continuous else args.idle_timeout,
        on_error=lambda error: print(f"Failed message: {error}", file=sys.stderr),
    )
    print(
        f"Consumed {result.consumed}; processed {result.processed}; "
        f"failed {result.failed}; topic {result.topic}; group {result.consumer_group}"
    )
    return 0 if result.failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
