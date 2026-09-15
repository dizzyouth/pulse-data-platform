"""Canonical Phase 6.3 economics inputs and contribution semantics."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import StrEnum
import math
import re
from typing import Any


class CostType(StrEnum):
    PRODUCT_COGS = "PRODUCT_COGS"
    SHIPPING = "SHIPPING"
    COD_FEE = "COD_FEE"
    FULFILLMENT = "FULFILLMENT"
    RETURN_FEE = "RETURN_FEE"
    PAYMENT_PROCESSING = "PAYMENT_PROCESSING"
    PROVIDER_FEE = "PROVIDER_FEE"
    OTHER_VARIABLE_COST = "OTHER_VARIABLE_COST"


class CostBasis(StrEnum):
    ACTUAL = "ACTUAL"
    CONFIGURED = "CONFIGURED"
    ESTIMATED = "ESTIMATED"


class CostScope(StrEnum):
    ORDER = "ORDER"
    SHIPMENT = "SHIPMENT"
    REMITTANCE = "REMITTANCE"
    SCHEDULE = "SCHEDULE"


class EconomicStatus(StrEnum):
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    INCOMPLETE_COSTS = "INCOMPLETE_COSTS"
    INCOMPLETE_ATTRIBUTION = "INCOMPLETE_ATTRIBUTION"
    CURRENCY_MISMATCH = "CURRENCY_MISMATCH"
    IMMATURE_COHORT = "IMMATURE_COHORT"


def _currency(value: str) -> str:
    if not re.fullmatch(r"[A-Z]{3}", value):
        raise ValueError("currency must be a three-letter uppercase code")
    return value


def _amount(value: float, name: str) -> float:
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{name} must be finite and nonnegative")
    return result


def _utc(value: datetime, name: str) -> datetime:
    if value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True, kw_only=True)
class ProductCost:
    business_id: str
    source_id: str
    provider: str
    cost_record_id: str
    external_record_id: str
    product_id: str | None
    variant_id: str | None
    sku: str | None
    unit_cogs: float
    currency: str
    valid_from: date
    valid_to: date | None
    revision: int = 1
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not all((self.business_id, self.source_id, self.provider,
                    self.cost_record_id, self.external_record_id)):
            raise ValueError("product-cost lineage and identity are required")
        if not any((self.product_id, self.variant_id, self.sku)):
            raise ValueError("product_id, variant_id, or sku is required")
        if self.valid_to is not None and self.valid_to < self.valid_from:
            raise ValueError("valid_to cannot precede valid_from")
        if self.revision < 1:
            raise ValueError("revision must be positive")
        object.__setattr__(self, "unit_cogs", _amount(self.unit_cogs, "unit_cogs"))
        _currency(self.currency)


@dataclass(frozen=True, slots=True, kw_only=True)
class CostComponent:
    business_id: str
    source_id: str
    provider: str
    cost_component_id: str
    external_record_id: str
    cost_type: CostType
    amount: float
    currency: str
    effective_at: datetime
    cost_basis: CostBasis
    cost_scope: CostScope
    precedence_key: str | None = None
    order_id: str | None = None
    shipment_id: str | None = None
    remittance_id: str | None = None
    product_id: str | None = None
    variant_id: str | None = None
    sku: str | None = None
    revision: int = 1
    corrects_record_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not all((self.business_id, self.source_id, self.provider,
                    self.cost_component_id, self.external_record_id)):
            raise ValueError("cost component lineage and identity are required")
        if not any((self.order_id, self.shipment_id, self.remittance_id,
                    self.product_id, self.variant_id, self.sku)):
            raise ValueError("cost component requires an economic subject")
        if self.revision < 1:
            raise ValueError("revision must be positive")
        object.__setattr__(self, "amount", _amount(self.amount, "amount"))
        object.__setattr__(self, "effective_at", _utc(self.effective_at, "effective_at"))
        _currency(self.currency)

    @property
    def priority(self) -> tuple[int, int]:
        basis = {CostBasis.ACTUAL: 3, CostBasis.CONFIGURED: 2, CostBasis.ESTIMATED: 1}
        scope = {CostScope.ORDER: 4, CostScope.SHIPMENT: 3,
                 CostScope.REMITTANCE: 2, CostScope.SCHEDULE: 1}
        return basis[self.cost_basis], scope[self.cost_scope]


@dataclass(frozen=True, slots=True, kw_only=True)
class AttributionLink:
    business_id: str
    source_id: str
    provider: str
    attribution_link_id: str
    external_record_id: str
    order_id: str
    marketing_platform: str
    marketing_source_id: str
    marketing_date: date
    attribution_method: str
    attribution_weight: float
    linked_at: datetime
    campaign_id: str | None = None
    ad_group_id: str | None = None
    ad_id: str | None = None
    revision: int = 1
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not all((self.business_id, self.source_id, self.provider,
                    self.attribution_link_id, self.external_record_id,
                    self.order_id, self.marketing_platform,
                    self.marketing_source_id, self.attribution_method)):
            raise ValueError("attribution identity and lineage are required")
        weight = float(self.attribution_weight)
        if not math.isfinite(weight) or not 0 <= weight <= 1:
            raise ValueError("attribution_weight must be between zero and one")
        if self.revision < 1:
            raise ValueError("revision must be positive")
        object.__setattr__(self, "attribution_weight", weight)
        object.__setattr__(self, "linked_at", _utc(self.linked_at, "linked_at"))


def safe_divide(numerator: float, denominator: float) -> float | None:
    return None if denominator == 0 else numerator / denominator


def contribution_value(recognized_value: float | None, variable_cost: float,
                       *, complete: bool) -> float | None:
    return None if recognized_value is None or not complete else recognized_value - variable_cost
