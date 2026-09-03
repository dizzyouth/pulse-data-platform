"""Publish generated Pulse marketplace events to Kafka."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from confluent_kafka import Producer

from .event_generator import MarketplaceEventGenerator
from .models import MarketplaceEvent

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_MARKETPLACE_TOPIC = "marketplace.events"


class ProducerClient(Protocol):
    """Subset of the Confluent producer API used by this module."""

    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        on_delivery: Any,
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float | None = None) -> int: ...


@dataclass(frozen=True, slots=True)
class PublishResult:
    attempted: int
    delivered: int
    topic: str
    bootstrap_servers: str


class KafkaPublishingError(RuntimeError):
    """Raised when one or more events cannot be confirmed as delivered."""

    def __init__(self, message: str, result: PublishResult) -> None:
        super().__init__(message)
        self.result = result


class MarketplaceKafkaPublisher:
    """Kafka transport adapter for marketplace events.

    Records use ``customer_id`` as their key so Kafka consistently assigns a
    customer's events to the same partition, preserving per-customer ordering
    within that partition.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        *,
        producer: ProducerClient | None = None,
        flush_timeout: float = 30.0,
    ) -> None:
        if not bootstrap_servers.strip():
            raise ValueError("bootstrap_servers cannot be empty")
        if not topic.strip():
            raise ValueError("topic cannot be empty")
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self._flush_timeout = flush_timeout
        self._producer: ProducerClient = producer or Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "client.id": "pulse-marketplace-producer",
            }
        )

    def publish(self, events: Iterable[MarketplaceEvent]) -> PublishResult:
        """Publish all events and return only after delivery is confirmed."""

        attempted = 0
        delivered = 0
        delivery_errors: list[str] = []
        publishing_error: Exception | None = None

        def on_delivery(error: Any, message: Any) -> None:
            nonlocal delivered
            if error is not None:
                delivery_errors.append(str(error))
            else:
                delivered += 1

        try:
            for event in events:
                attempted += 1
                value = json.dumps(
                    event.to_dict(), separators=(",", ":"), ensure_ascii=False
                ).encode("utf-8")
                key = event.customer_id.encode("utf-8")
                while True:
                    try:
                        self._producer.produce(
                            self.topic,
                            key=key,
                            value=value,
                            on_delivery=on_delivery,
                        )
                        break
                    except BufferError:
                        self._producer.poll(1.0)
                self._producer.poll(0)
        except Exception as error:  # Always flush events queued before this failure.
            publishing_error = error

        outstanding = 0
        try:
            outstanding = self._producer.flush(self._flush_timeout)
        except Exception as error:
            publishing_error = publishing_error or error

        result = PublishResult(
            attempted=attempted,
            delivered=delivered,
            topic=self.topic,
            bootstrap_servers=self.bootstrap_servers,
        )
        failures = attempted - delivered
        if publishing_error is not None:
            raise KafkaPublishingError(
                f"Kafka publishing failed after {attempted} attempted event(s): "
                f"{publishing_error}",
                result,
            ) from publishing_error
        if outstanding:
            raise KafkaPublishingError(
                f"Kafka flush timed out with {outstanding} message(s) still queued; "
                f"delivered {delivered} of {attempted}",
                result,
            )
        if delivery_errors or failures:
            details = "; ".join(delivery_errors[:3]) or "delivery was not acknowledged"
            raise KafkaPublishingError(
                f"Kafka failed to deliver {failures} of {attempted} event(s): {details}",
                result,
            )
        return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate Pulse marketplace journeys and publish them to Kafka"
    )
    parser.add_argument("--journeys", type=int, default=1, help="journeys to publish")
    parser.add_argument("--seed", type=int, help="optional seed for reproducible values")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.journeys < 0:
        raise SystemExit("journeys must be zero or greater")

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS
    )
    topic = os.getenv("KAFKA_MARKETPLACE_TOPIC", DEFAULT_MARKETPLACE_TOPIC)
    publisher = MarketplaceKafkaPublisher(bootstrap_servers, topic)
    events = MarketplaceEventGenerator(seed=args.seed).generate_journeys(args.journeys)

    try:
        result = publisher.publish(events)
    except KafkaPublishingError as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    print(
        f"Published {result.delivered}/{result.attempted} events "
        f"to {result.topic} via {result.bootstrap_servers}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
