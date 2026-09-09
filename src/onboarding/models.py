"""Immutable business, source, and ingestion identity models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import re
from typing import Any


IDENTIFIER_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{2,63}$")


@dataclass(frozen=True, slots=True, kw_only=True)
class BusinessConfig:
    business_id: str
    business_name: str
    status: str
    timezone: str
    currency: str
    country: str
    enabled_sources: tuple[str, ...]
    reporting_timezone: str
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, value: dict[str, Any]):
        return cls(**{**value, "enabled_sources": tuple(value.get("enabled_sources", ()))})


@dataclass(frozen=True, slots=True, kw_only=True)
class SourceConfig:
    source_type: str
    source_id: str
    enabled: bool
    business_id: str
    ingestion_mode: str
    schedule: str
    schema_version: str
    credential_ref: str
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, value: dict[str, Any]):
        return cls(**value)


@dataclass(frozen=True, slots=True, kw_only=True)
class IngestionEnvelope:
    business_id: str
    source_type: str
    source_id: str
    ingestion_id: str
    record_id: str
    extracted_at_utc: datetime
    schema_version: str
    payload: dict[str, Any]
    source_updated_at_utc: datetime | None = None

    def __post_init__(self):
        for name in ("business_id", "source_type", "source_id"):
            if not IDENTIFIER_PATTERN.fullmatch(getattr(self, name)):
                raise ValueError(f"{name} must be a stable lowercase identifier")
        if not self.ingestion_id.strip() or not self.record_id.strip():
            raise ValueError("ingestion_id and record_id are required")
        for name in ("extracted_at_utc", "source_updated_at_utc"):
            value = getattr(self, name)
            if value is not None:
                if value.utcoffset() is None:
                    raise ValueError(f"{name} must be timezone-aware")
                object.__setattr__(self, name, value.astimezone(timezone.utc))
        if not self.schema_version.strip() or not isinstance(self.payload, dict):
            raise ValueError("schema_version and object payload are required")

    def to_dict(self):
        return {
            "business_id": self.business_id, "source_type": self.source_type,
            "source_id": self.source_id, "ingestion_id": self.ingestion_id,
            "record_id": self.record_id,
            "extracted_at_utc": self.extracted_at_utc.isoformat().replace("+00:00", "Z"),
            "source_updated_at_utc": (self.source_updated_at_utc.isoformat().replace("+00:00", "Z")
                                      if self.source_updated_at_utc else None),
            "schema_version": self.schema_version, "payload": self.payload,
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class ValidationIssue:
    code: str
    path: str
    message: str


@dataclass(frozen=True, slots=True, kw_only=True)
class ValidationReport:
    business_id: str
    issues: tuple[ValidationIssue, ...]
    source_count: int

    @property
    def valid(self):
        return not self.issues

    def to_dict(self):
        return {"business_id": self.business_id, "valid": self.valid,
                "source_count": self.source_count,
                "issues": [{"code": issue.code, "path": issue.path, "message": issue.message}
                           for issue in self.issues]}
