"""Strict immutable contracts for deterministic investigation opportunities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
import math
import re
from typing import Any


class OpportunityValidationError(ValueError):
    """An opportunity failed its deterministic local contract."""


class OpportunityScope(StrEnum):
    BUSINESS = "BUSINESS"
    CAMPAIGN = "CAMPAIGN"


class OpportunityType(StrEnum):
    ACQUISITION_FULFILLMENT_MISALIGNMENT = "ACQUISITION_FULFILLMENT_MISALIGNMENT"
    CONFIRMATION_LEAKAGE = "CONFIRMATION_LEAKAGE"
    MEASUREMENT_RECONCILIATION = "MEASUREMENT_RECONCILIATION"
    ACQUISITION_EFFICIENCY_REVIEW = "ACQUISITION_EFFICIENCY_REVIEW"


class OpportunityCategory(StrEnum):
    CROSS_DOMAIN = "CROSS_DOMAIN"
    CONFIRMATION = "CONFIRMATION"
    MEASUREMENT = "MEASUREMENT"
    ACQUISITION = "ACQUISITION"


class OpportunityPriority(StrEnum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class OpportunityConfidence(StrEnum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class HypothesisStatus(StrEnum):
    UNTESTED = "UNTESTED"


class SuppressionReason(StrEnum):
    INSUFFICIENT_SAMPLE = "INSUFFICIENT_SAMPLE"
    INSUFFICIENT_MATERIALITY = "INSUFFICIENT_MATERIALITY"
    CONTRADICTORY_EVIDENCE = "CONTRADICTORY_EVIDENCE"
    MISSING_REQUIRED_DOMAIN = "MISSING_REQUIRED_DOMAIN"
    ECONOMIC_BOUNDARY = "ECONOMIC_BOUNDARY"
    DATA_QUALITY_BLOCKER = "DATA_QUALITY_BLOCKER"


_OPPORTUNITY_ID = re.compile(r"^opportunity:[a-z0-9_-]+:[a-z0-9_-]+$")


def valid_opportunity_id_shape(value: str) -> bool:
    """Return whether *value* has the bounded engine-owned stable ID shape."""
    return isinstance(value, str) and bool(_OPPORTUNITY_ID.fullmatch(value))


def _text(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise OpportunityValidationError(f"{field} must be non-empty text")
    return value.strip()


def _text_tuple(value: tuple[str, ...], field: str, *, required: bool = False) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise OpportunityValidationError(f"{field} must be a tuple")
    normalized = tuple(_text(item, field) for item in value)
    if required and not normalized:
        raise OpportunityValidationError(f"{field} cannot be empty")
    if len(normalized) != len(set(normalized)):
        raise OpportunityValidationError(f"{field} cannot contain duplicates")
    return normalized


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9_-]+", "-", value.strip().lower()).strip("-_")
    if not slug:
        raise OpportunityValidationError("Opportunity identity cannot be empty")
    return slug


def stable_opportunity_id(
    opportunity_type: OpportunityType | str,
    scope_type: OpportunityScope | str,
    scope_name: str,
) -> str:
    kind = OpportunityType(opportunity_type).value.lower()
    OpportunityScope(scope_type)
    suffix = _slug(scope_name)
    return f"opportunity:{kind}:{suffix}"


@dataclass(frozen=True, slots=True, kw_only=True)
class InvestigationOpportunity:
    opportunity_order: int
    opportunity_id: str
    business_id: str
    as_of_date: date
    scope_type: OpportunityScope
    scope_id: str | None
    scope_name: str
    opportunity_type: OpportunityType
    category: OpportunityCategory
    title: str
    observation_summary: str
    hypothesis_to_test: str
    hypothesis_status: HypothesisStatus
    priority: OpportunityPriority
    confidence: OpportunityConfidence
    impact_proxy_name: str | None
    impact_proxy_value: float | None
    impact_proxy_unit: str | None
    supporting_evidence_refs: tuple[str, ...]
    counter_evidence_refs: tuple[str, ...]
    blocking_evidence_refs: tuple[str, ...]
    investigation_steps: tuple[str, ...]
    confirmation_criteria: tuple[str, ...]
    refutation_criteria: tuple[str, ...]
    decision_unlocked: str
    missing_evidence: tuple[str, ...]
    limitation: str
    causal_claim: bool = False

    def __post_init__(self) -> None:
        if (
            isinstance(self.opportunity_order, bool)
            or not isinstance(self.opportunity_order, int)
            or self.opportunity_order < 0
        ):
            raise OpportunityValidationError("opportunity_order must be a non-negative integer")
        if isinstance(self.as_of_date, datetime) or not isinstance(self.as_of_date, date):
            raise OpportunityValidationError("as_of_date must be a date")
        try:
            object.__setattr__(self, "scope_type", OpportunityScope(self.scope_type))
            object.__setattr__(self, "opportunity_type", OpportunityType(self.opportunity_type))
            object.__setattr__(self, "category", OpportunityCategory(self.category))
            object.__setattr__(self, "hypothesis_status", HypothesisStatus(self.hypothesis_status))
            object.__setattr__(self, "priority", OpportunityPriority(self.priority))
            object.__setattr__(self, "confidence", OpportunityConfidence(self.confidence))
        except ValueError as exc:
            raise OpportunityValidationError(str(exc)) from None
        for field in (
            "opportunity_id", "business_id", "scope_name", "title",
            "observation_summary", "hypothesis_to_test", "decision_unlocked",
            "limitation",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        if self.scope_id is not None:
            object.__setattr__(self, "scope_id", _text(self.scope_id, "scope_id"))
        if self.scope_type is OpportunityScope.CAMPAIGN and self.scope_id is None:
            raise OpportunityValidationError("Campaign opportunities require scope_id")
        expected_id = stable_opportunity_id(
            self.opportunity_type, self.scope_type, self.scope_name
        )
        if not _OPPORTUNITY_ID.fullmatch(self.opportunity_id) or self.opportunity_id != expected_id:
            raise OpportunityValidationError("opportunity_id is not the stable expected ID")
        for field, required in (
            ("supporting_evidence_refs", True),
            ("counter_evidence_refs", False),
            ("blocking_evidence_refs", False),
            ("investigation_steps", True),
            ("confirmation_criteria", True),
            ("refutation_criteria", True),
            ("missing_evidence", False),
        ):
            object.__setattr__(
                self, field, _text_tuple(getattr(self, field), field, required=required)
            )
        buckets = (
            set(self.supporting_evidence_refs), set(self.counter_evidence_refs),
            set(self.blocking_evidence_refs),
        )
        if any(buckets[left] & buckets[right] for left, right in ((0, 1), (0, 2), (1, 2))):
            raise OpportunityValidationError("Evidence buckets must be disjoint")
        impact = (
            self.impact_proxy_name, self.impact_proxy_value, self.impact_proxy_unit
        )
        if any(value is None for value in impact) and any(value is not None for value in impact):
            raise OpportunityValidationError("Impact proxy fields must be supplied together")
        if self.impact_proxy_name is not None:
            object.__setattr__(
                self, "impact_proxy_name", _text(self.impact_proxy_name, "impact_proxy_name")
            )
            object.__setattr__(
                self, "impact_proxy_unit", _text(self.impact_proxy_unit, "impact_proxy_unit")
            )
            if (
                isinstance(self.impact_proxy_value, bool)
                or not isinstance(self.impact_proxy_value, (int, float))
                or not math.isfinite(float(self.impact_proxy_value))
                or float(self.impact_proxy_value) < 0
            ):
                raise OpportunityValidationError("impact_proxy_value must be finite and non-negative")
            object.__setattr__(self, "impact_proxy_value", float(self.impact_proxy_value))
        if self.causal_claim is not False:
            raise OpportunityValidationError("Investigation opportunities cannot make causal claims")

    def to_dict(self) -> dict[str, Any]:
        return {
            "opportunity_order": self.opportunity_order,
            "opportunity_id": self.opportunity_id,
            "business_id": self.business_id,
            "as_of_date": self.as_of_date.isoformat(),
            "scope_type": self.scope_type.value,
            "scope_id": self.scope_id,
            "scope_name": self.scope_name,
            "opportunity_type": self.opportunity_type.value,
            "category": self.category.value,
            "title": self.title,
            "observation_summary": self.observation_summary,
            "hypothesis_to_test": self.hypothesis_to_test,
            "hypothesis_status": self.hypothesis_status.value,
            "priority": self.priority.value,
            "confidence": self.confidence.value,
            "impact_proxy_name": self.impact_proxy_name,
            "impact_proxy_value": self.impact_proxy_value,
            "impact_proxy_unit": self.impact_proxy_unit,
            "supporting_evidence_refs": list(self.supporting_evidence_refs),
            "counter_evidence_refs": list(self.counter_evidence_refs),
            "blocking_evidence_refs": list(self.blocking_evidence_refs),
            "investigation_steps": list(self.investigation_steps),
            "confirmation_criteria": list(self.confirmation_criteria),
            "refutation_criteria": list(self.refutation_criteria),
            "decision_unlocked": self.decision_unlocked,
            "missing_evidence": list(self.missing_evidence),
            "limitation": self.limitation,
            "causal_claim": self.causal_claim,
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class OpportunityCandidate:
    rule_id: str
    opportunity: InvestigationOpportunity

    def __post_init__(self) -> None:
        object.__setattr__(self, "rule_id", _text(self.rule_id, "rule_id"))
        if not isinstance(self.opportunity, InvestigationOpportunity):
            raise OpportunityValidationError("candidate opportunity is invalid")


@dataclass(frozen=True, slots=True, kw_only=True)
class SuppressedOpportunity:
    rule_id: str
    scope_type: OpportunityScope
    scope_id: str | None
    scope_name: str
    reason_code: SuppressionReason
    evidence_refs: tuple[str, ...]
    explanation: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "rule_id", _text(self.rule_id, "rule_id"))
        object.__setattr__(self, "scope_type", OpportunityScope(self.scope_type))
        object.__setattr__(self, "scope_name", _text(self.scope_name, "scope_name"))
        object.__setattr__(self, "reason_code", SuppressionReason(self.reason_code))
        object.__setattr__(self, "explanation", _text(self.explanation, "explanation"))
        object.__setattr__(
            self, "evidence_refs", _text_tuple(self.evidence_refs, "evidence_refs")
        )
        if self.scope_id is not None:
            object.__setattr__(self, "scope_id", _text(self.scope_id, "scope_id"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "scope_type": self.scope_type.value,
            "scope_id": self.scope_id,
            "scope_name": self.scope_name,
            "reason_code": self.reason_code.value,
            "evidence_refs": list(self.evidence_refs),
            "explanation": self.explanation,
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class OpportunityEvaluation:
    business_id: str
    as_of_date: date
    opportunities: tuple[InvestigationOpportunity, ...]
    suppressed_candidates: tuple[SuppressedOpportunity, ...]

    def to_dict(self, *, include_suppressed: bool = False) -> dict[str, Any]:
        value: dict[str, Any] = {
            "business_id": self.business_id,
            "as_of_date": self.as_of_date.isoformat(),
            "opportunities": [item.to_dict() for item in self.opportunities],
        }
        if include_suppressed:
            value["suppressed_candidates"] = [
                item.to_dict() for item in self.suppressed_candidates
            ]
        return value
