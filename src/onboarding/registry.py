"""Version-controlled business/source registry and structured validation."""

from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path
import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from src.onboarding.contracts import SUPPORTED_SOURCE_TYPES, contract_for, SourceContractError
from src.onboarding.models import (BusinessConfig, IDENTIFIER_PATTERN, SourceConfig,
                                   ValidationIssue, ValidationReport)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BUSINESSES_DIR = PROJECT_ROOT / "config" / "businesses"
DEFAULT_SOURCES_DIR = PROJECT_ROOT / "config" / "sources"
CRON_PATTERN = re.compile(r"^[0-9*/?,\-]+$")
CREDENTIAL_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]{2,127}$")


class RegistryError(ValueError):
    pass


class BusinessRegistry:
    def __init__(self, businesses_dir: Path | None = None, sources_dir: Path | None = None):
        self.businesses_dir = Path(os.getenv("PULSE_BUSINESSES_DIR", businesses_dir or DEFAULT_BUSINESSES_DIR))
        self.sources_dir = Path(os.getenv("PULSE_SOURCES_DIR", sources_dir or DEFAULT_SOURCES_DIR))

    @staticmethod
    def _read(path: Path):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise RegistryError(f"Cannot read registry file {path.name}: {type(error).__name__}") from None
        if not isinstance(value, dict):
            raise RegistryError(f"Registry file {path.name} must contain one object")
        return value

    def list_businesses(self):
        if not self.businesses_dir.exists():
            return ()
        return tuple(sorted((BusinessConfig.from_dict(self._read(path))
                            for path in self.businesses_dir.glob("*.json")),
                            key=lambda item: item.business_id))

    def get_business(self, business_id: str):
        matches = [item for item in self.list_businesses() if item.business_id == business_id]
        if len(matches) != 1:
            raise RegistryError(f"Business {business_id!r} was not found exactly once")
        return matches[0]

    def list_sources(self):
        if not self.sources_dir.exists():
            return ()
        return tuple(sorted((SourceConfig.from_dict(self._read(path))
                            for path in self.sources_dir.glob("*.json")),
                            key=lambda item: (item.business_id, item.source_id)))

    def sources_for(self, business_id: str):
        return tuple(item for item in self.list_sources() if item.business_id == business_id)

    def show(self, business_id: str):
        business = self.get_business(business_id)
        return {"business": asdict(business),
                "sources": [asdict(source) for source in self.sources_for(business_id)]}


def _valid_schedule(value: str):
    parts = value.split()
    return len(parts) == 5 and all(CRON_PATTERN.fullmatch(part) for part in parts)


def validate_business(registry: BusinessRegistry, business_id: str):
    issues = []
    try:
        business = registry.get_business(business_id)
    except RegistryError as error:
        return ValidationReport(business_id=business_id,
                                issues=(ValidationIssue(code="business_not_found", path="business_id",
                                                        message=str(error)),), source_count=0)
    def issue(code, path, message):
        issues.append(ValidationIssue(code=code, path=path, message=message))
    if not IDENTIFIER_PATTERN.fullmatch(business.business_id):
        issue("invalid_business_id", "business.business_id", "Use a stable lowercase identifier")
    if not business.business_name.strip() or business.status not in ("ACTIVE", "INACTIVE"):
        issue("invalid_business_metadata", "business", "Name and ACTIVE/INACTIVE status are required")
    for field, value in (("timezone", business.timezone), ("reporting_timezone", business.reporting_timezone)):
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError):
            issue("invalid_timezone", f"business.{field}", f"Unknown timezone {value!r}")
    if not re.fullmatch(r"[A-Z]{3}", business.currency):
        issue("invalid_currency", "business.currency", "Currency must be three uppercase letters")
    if not re.fullmatch(r"[A-Z]{2}", business.country):
        issue("invalid_country", "business.country", "Country must be two uppercase letters")
    sources = registry.sources_for(business_id)
    by_id = {}
    for source in sources:
        by_id.setdefault(source.source_id, []).append(source)
    for source_id, matches in by_id.items():
        if len(matches) > 1:
            issue("duplicate_source_id", f"sources.{source_id}", "Source IDs must be unique per business")
    configured = set(by_id)
    if len(set(business.enabled_sources)) != len(business.enabled_sources):
        issue("duplicate_enabled_source", "business.enabled_sources",
              "Enabled source IDs must not repeat")
    for source_id in business.enabled_sources:
        if source_id not in configured:
            issue("enabled_source_missing", f"business.enabled_sources.{source_id}",
                  "Enabled source config does not exist")
    for source in sources:
        prefix = f"sources.{source.source_id}"
        if source.source_type not in SUPPORTED_SOURCE_TYPES:
            issue("unsupported_source_type", prefix + ".source_type", "Unknown source type")
        if source.enabled and source.source_id not in business.enabled_sources:
            issue("enabled_source_unlisted", prefix + ".enabled", "Enabled source is absent from business list")
        if not source.enabled and source.source_id in business.enabled_sources:
            issue("listed_source_disabled", prefix + ".enabled",
                  "Business lists this source as enabled but the source config is disabled")
        if not IDENTIFIER_PATTERN.fullmatch(source.source_id):
            issue("invalid_source_id", prefix + ".source_id", "Use a stable lowercase identifier")
        if source.ingestion_mode not in ("batch", "file"):
            issue("invalid_ingestion_mode", prefix + ".ingestion_mode", "Expected batch or file")
        if not _valid_schedule(source.schedule):
            issue("invalid_schedule", prefix + ".schedule", "Expected a five-field cron expression")
        if not CREDENTIAL_PATTERN.fullmatch(source.credential_ref or ""):
            issue("missing_credential_ref", prefix + ".credential_ref",
                  "Declare an uppercase environment-variable reference")
        try:
            contract_for(source.source_type, source.schema_version)
        except SourceContractError as error:
            issue("unsupported_schema_version", prefix + ".schema_version", str(error))
    return ValidationReport(business_id=business_id, issues=tuple(issues), source_count=len(sources))
