"""Offline economics input adapters using the existing SourceAdapter boundary."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from src.economics.models import (AttributionLink, CostBasis, CostComponent,
                                  CostScope, CostType, ProductCost)
from src.onboarding.adapters import AdapterHealth, MockSourceAdapter
from src.onboarding.contracts import contract_for
from src.onboarding.models import IngestionEnvelope, SourceConfig


PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = PROJECT_ROOT / "data" / "fixtures" / "economics"
EXTRACTED_AT = datetime(2026, 2, 15, 12, tzinfo=timezone.utc)
ECONOMICS_SOURCE_TYPES = frozenset({"product_costs", "variable_cost_events", "attribution_links"})


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() is None:
        raise ValueError("economics timestamps must include timezone")
    return parsed.astimezone(timezone.utc)


class CommerceEconomicsSourceAdapter(MockSourceAdapter):
    def validate_config(self) -> tuple[str, ...]:
        errors = list(super().validate_config())
        if self.config.source_type not in ECONOMICS_SOURCE_TYPES:
            errors.append("source is not an economics enrichment role")
        if self.config.metadata.get("adapter", "mock") != "mock":
            errors.append("economics adapters are offline mock-only in Phase 6.3")
        fixture = str(self.config.metadata.get("fixture", ""))
        if not fixture or Path(fixture).name != fixture or not (FIXTURE_ROOT / fixture).is_file():
            errors.append("metadata.fixture must name an economics fixture")
        return tuple(errors)

    def raw_payloads(self) -> tuple[dict[str, Any], ...]:
        value = json.loads((FIXTURE_ROOT / self.config.metadata["fixture"]).read_text(encoding="utf-8"))
        if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
            raise ValueError("economics fixture must contain an object array")
        return tuple(value)

    def extract(self) -> tuple[IngestionEnvelope, ...]:
        errors = self.validate_config()
        if errors:
            raise ValueError("; ".join(errors))
        contract = contract_for(self.config.source_type, self.config.schema_version)
        ingestion_id = str(uuid5(NAMESPACE_URL,
            f"economics-ingestion|{self.config.business_id}|{self.config.source_id}"))
        output = []
        for payload in self.raw_payloads():
            contract.validate(payload)
            revision = int(payload.get("revision", 1))
            logical = "|".join((self.config.business_id, self.config.source_id,
                                payload["provider"], payload["external_record_id"], str(revision)))
            output.append(IngestionEnvelope(
                business_id=self.config.business_id, source_type=self.config.source_type,
                source_id=self.config.source_id, ingestion_id=ingestion_id,
                record_id=str(uuid5(NAMESPACE_URL, f"economics-record|{logical}")),
                extracted_at_utc=EXTRACTED_AT,
                source_updated_at_utc=_utc(payload[contract.source_timestamp]),
                schema_version=self.config.schema_version, payload=dict(payload)))
        return tuple(output)

    def normalize(self, record: IngestionEnvelope) -> dict[str, Any]:
        if (record.business_id, record.source_type, record.source_id) != (
                self.config.business_id, self.config.source_type, self.config.source_id):
            raise ValueError("Envelope identity does not match adapter configuration")
        contract_for(record.source_type, record.schema_version).validate(record.payload)
        p = dict(record.payload)
        details = p.pop("details", {})
        p.pop("received_at_utc", None)
        p.pop("updated_at_utc", None)
        common = {"business_id": record.business_id, "source_id": record.source_id,
                  "provider": p.pop("provider"), "external_record_id": p.pop("external_record_id"),
                  "revision": int(p.pop("revision", 1)), "details": details}
        if record.source_type == "product_costs":
            for name in ("product_id", "variant_id", "sku"):
                p.setdefault(name, None)
            entity = ProductCost(cost_record_id=record.record_id, **common,
                valid_from=datetime.fromisoformat(p.pop("valid_from")).date(),
                valid_to=(datetime.fromisoformat(p.pop("valid_to")).date() if p.get("valid_to") else None), **p)
        elif record.source_type == "variable_cost_events":
            for name in ("precedence_key", "order_id", "shipment_id", "remittance_id",
                         "product_id", "variant_id", "sku", "corrects_record_id"):
                p.setdefault(name, None)
            entity = CostComponent(cost_component_id=record.record_id, **common,
                cost_type=CostType(p.pop("cost_type")), cost_basis=CostBasis(p.pop("cost_basis")),
                cost_scope=CostScope(p.pop("cost_scope")), effective_at=_utc(p.pop("effective_at")), **p)
        else:
            for name in ("campaign_id", "ad_group_id", "ad_id"):
                p.setdefault(name, None)
            entity = AttributionLink(attribution_link_id=record.record_id, **common,
                marketing_date=datetime.fromisoformat(p.pop("marketing_date")).date(),
                linked_at=_utc(p.pop("linked_at")), **p)
        return {name: getattr(entity, name) for name in entity.__dataclass_fields__}

    def healthcheck(self) -> AdapterHealth:
        errors = self.validate_config()
        return AdapterHealth(healthy=not errors,
            message="; ".join(errors) if errors else "offline economics fixture; no network access")


def economics_adapter_for(config: SourceConfig) -> CommerceEconomicsSourceAdapter:
    if config.source_type not in ECONOMICS_SOURCE_TYPES:
        raise ValueError(f"Source type {config.source_type!r} is not an economics source")
    return CommerceEconomicsSourceAdapter(config)
