"""Provider-neutral alert notification contracts.

The default provider writes a small structured log record only. It performs no
network I/O and is safe for local development and CI.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
import logging
from typing import Any, Mapping, Protocol
from uuid import UUID


FINAL_DELIVERY_STATUSES = ("SENT", "FAILED", "SKIPPED")


@dataclass(frozen=True)
class AlertNotification:
    alert_event_id: UUID
    severity: str
    lifecycle_status: str
    title: str
    message: str
    dataset_name: str
    layer: str
    first_seen_at_utc: datetime
    last_seen_at_utc: datetime
    occurrence_count: int


@dataclass(frozen=True)
class DeliveryContext:
    logical_delivery_key: str
    delivery_kind: str
    delivery_version: int
    escalation_level: int
    destination_key: str
    attempt_number: int
    attempted_at_utc: datetime


@dataclass(frozen=True)
class DeliveryResult:
    status: str
    external_reference: str | None = None
    error_message: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in FINAL_DELIVERY_STATUSES:
            raise ValueError(f"Provider returned invalid delivery status: {self.status}")


class NotificationProvider(Protocol):
    name: str

    def send(self, alert: AlertNotification, context: DeliveryContext) -> DeliveryResult:
        """Deliver one notification attempt and return a final result."""


class LoggingProvider:
    """Safe default provider that emits identifiers and routing metadata only."""

    name = "log"

    def send(self, alert: AlertNotification, context: DeliveryContext) -> DeliveryResult:
        logging.getLogger("pulse.alert_delivery").info(
            json.dumps({
                "event": "alert_delivery",
                "alert_event_id": str(alert.alert_event_id),
                "severity": alert.severity,
                "lifecycle_status": alert.lifecycle_status,
                "dataset_name": alert.dataset_name,
                "layer": alert.layer,
                "delivery_kind": context.delivery_kind,
                "delivery_version": context.delivery_version,
                "escalation_level": context.escalation_level,
                "destination_key": context.destination_key,
                "attempt_number": context.attempt_number,
            }, sort_keys=True)
        )
        return DeliveryResult(status="SENT", details={"transport": "local-log"})
